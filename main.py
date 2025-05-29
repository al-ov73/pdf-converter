from fastapi import FastAPI, Request
import io
import json
from urllib.parse import quote
from fastapi.templating import Jinja2Templates
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from base64 import b64encode
from pathlib import Path
from fastapi.staticfiles import StaticFiles

from middleware import SessionMiddleware
from utils import (
    combine_archives,
    convert_and_pack,
    convert_pdf_to_images,
    get_files_from_session,
    merge_pdfs,
    rotate_pages_in_pdf,
    split_pdf,
)

app = FastAPI()
app.add_middleware(SessionMiddleware)

ORIGINS="http://localhost:3000,http://127.0.0.1:3000,http://45.80.71.178:3000,http://45.80.71.178,http://45.80.71.178:80,http://www.memovoz.store,http://memovoz.store,http://www.memovoz.ru,http://memovoz.ru,https://www.memovoz.ru,https://memovoz.ru,https://memovoz.ru/"

app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS.split(','),
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS", "DELETE", "PATCH", "PUT"],
    allow_headers=["*"],
)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.filters["b64encode"] = lambda x: b64encode(x).decode("utf-8")

STATIC_DIR = "static"
STATIC_URL = "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), STATIC_URL)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "session": request.state.session,
        },
    )


@app.post("/upload")
async def upload_pdf(request: Request, file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Только файлы формата PDF разрешены.")

    content = await file.read()
    memory_file = io.BytesIO(content)

    file_previews = convert_pdf_to_images(memory_file.getvalue())
    filename = file.filename

    new_file = {
        "filename": filename,
        "file_content": memory_file.getvalue(),
        "file_previews": file_previews,
    }

    files = request.state.session.get("files", {})
    files[filename] = new_file
    request.state.session["files"] = files

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "session": request.state.session,
        },
    )


@app.get("/split", response_class=HTMLResponse)
async def split_page(request: Request):
    return templates.TemplateResponse(
        "split.html",
        {
            "request": request,
            "session": request.state.session,
        },
    )


@app.post("/split-pdf")
async def split_pdf_page(
    request: Request,
    original_filename: str = Form(...),
    pages: str = Form(...),
    output_name: str = Form("output.pdf"),
):
    """
    Обрабатывает запрос на разделение PDF

    Args:
        request: FastAPI Request объект
        original_filename: Имя исходного файла из формы
        pages: Строка с номерами страниц для извлечения
        output_name: Имя результирующего файла

    Returns:
        StreamingResponse: Ответ с PDF файлом
    """
    # Получаем файлы из сессии
    files = request.state.session.get("files", {})

    # Проверяем наличие файла
    if original_filename not in files:
        raise HTTPException(status_code=400, detail="Запрошенный файл не найден в сессии")

    # Получаем содержимое файла
    file_data = files[original_filename]
    file_content = file_data.get("file_content")
    if not file_content:
        raise HTTPException(status_code=400, detail="Отсутствует содержимое PDF файла")

    # Используем нашу функцию для разделения PDF
    try:
        output_stream = split_pdf(file_content, pages)
    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")

    # Возвращаем результат
    return StreamingResponse(
        output_stream,
        headers={"Content-Disposition": f"attachment; filename={quote(output_name)}"},
        media_type="application/pdf",
    )


@app.get("/merge", response_class=HTMLResponse)
async def merge_page(request: Request):
    return templates.TemplateResponse(
        "merge.html",
        {
            "request": request,
            "session": request.state.session,
        },
    )


@app.post("/merge-pdfs")
async def merge_pdfs_page(request: Request, filenames: list = Form(...), output_name: str = Form("merged.pdf")):
    session_files = request.state.session.get("files", [])
    merged_bytes = merge_pdfs(filenames, session_files)
    return StreamingResponse(
        merged_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={output_name}"},
    )


@app.get("/convert", response_class=HTMLResponse)
async def convert_page(request: Request):
    return templates.TemplateResponse(
        "convert.html",
        {
            "request": request,
            "session": request.state.session,
        },
    )


@app.post("/convert-pdf-to-jpg")
async def convert_pdf_to_jpg(
    request: Request,
    filenames: list = Form(...),
    dpi: int = Form(300),
    output_name: str = Form("converted"),
):
    requested_files = get_files_from_session(request, filenames)

    individual_archives = []
    for filename, file_content in requested_files.items():
        individual_archive = convert_and_pack(filename, file_content, dpi)
        individual_archives.append(individual_archive)

    combined_archive = combine_archives(individual_archives)

    return StreamingResponse(
        combined_archive,
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={output_name}.zip"},
    )


@app.get("/rotate", response_class=HTMLResponse)
async def rotate_page(request: Request):
    return templates.TemplateResponse(
        "rotate.html",
        {"request": request, "session": request.state.session},
    )


@app.post("/rotate-pdf")
async def rotate_pdf_pages(
    request: Request,
    original_filename: str = Form(...),
    pages_and_angles: str = Form(...),
    output_name: str = Form("rotated.pdf"),
):
    """
    Обрабатывает запрос на поворот страниц PDF
    """
    # Проверка наличия обязательных данных
    if not pages_and_angles or pages_and_angles.strip() == "":
        raise HTTPException(status_code=400, detail="Необходимо выбрать страницы и установить углы поворота.")

    # Парсим JSON-данные
    try:
        pages_and_angles_dict = json.loads(pages_and_angles)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Неправильный формат JSON")

    # Получаем файлы из сессии
    files = request.state.session.get("files", {})

    # Проверяем наличие файла
    if original_filename not in files:
        raise HTTPException(status_code=400, detail="Запрошенный файл не найден в сессии")

    # Получаем содержимое файла
    file_data = files[original_filename]
    file_content = file_data.get("file_content")
    if not file_content:
        raise HTTPException(status_code=400, detail="Отсутствует содержимое PDF файла")

    # Преобразуем данные в нужный формат
    rotations = [(int(page), angle) for page, angle in pages_and_angles_dict.items() if angle != 0]

    # Применяем поворот страниц
    try:
        rotated_pdf = rotate_pages_in_pdf(file_content, rotations)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    # Возвращаем результат
    return StreamingResponse(
        rotated_pdf,
        headers={"Content-Disposition": f"attachment; filename={output_name}"},
        media_type="application/pdf",
    )