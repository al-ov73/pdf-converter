from fastapi import FastAPI, Request
import io
import json
from urllib.parse import quote
from fastapi.templating import Jinja2Templates
from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse
from base64 import b64encode
from pathlib import Path
from fastapi.staticfiles import StaticFiles

from logger import get_logger
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

logger = get_logger(__name__)

app = FastAPI()
app.add_middleware(SessionMiddleware)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.filters["b64encode"] = lambda x: b64encode(x).decode("utf-8")

STATIC_DIR = "static"
STATIC_URL = "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), STATIC_URL)


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    logger.info("Handling request for home page")
    try:
        response = templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "session": request.state.session,
            },
        )
        logger.info("Successfully rendered home page")
        return response
    except Exception as e:
        logger.error(f"Error rendering home page: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/upload")
async def upload_pdf(request: Request, file: UploadFile = File(...)):
    logger.info(f"Starting file upload: {file.filename}")
    if not file.filename.lower().endswith(".pdf"):
        logger.warning(f"Invalid file format attempted: {file.filename}")
        raise HTTPException(status_code=400, detail="Только файлы формата PDF разрешены.")

    try:
        content = await file.read()
        memory_file = io.BytesIO(content)

        logger.debug("Converting PDF to images for preview")
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
        logger.info(f"Successfully uploaded and processed file: {filename}")

        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "session": request.state.session,
            },
        )
    except Exception as e:
        logger.error(f"Error processing uploaded file {file.filename}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/split", response_class=HTMLResponse)
async def split_page(request: Request):
    logger.info("Handling request for split page")
    try:
        response = templates.TemplateResponse(
            "split.html",
            {
                "request": request,
                "session": request.state.session,
            },
        )
        logger.info("Successfully rendered split page")
        return response
    except Exception as e:
        logger.error(f"Error rendering split page: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/split-pdf")
async def split_pdf_page(
        request: Request,
        original_filename: str = Form(...),
        pages: str = Form(...),
        output_name: str = Form("output.pdf"),
):
    """
    Обрабатывает запрос на разделение PDF
    """
    logger.info(f"Starting PDF split operation for file: {original_filename}, pages: {pages}")

    files = request.state.session.get("files", {})
    logger.debug(f"Session contains {len(files)} files")

    if original_filename not in files:
        logger.warning(f"File not found in session: {original_filename}")
        raise HTTPException(status_code=400, detail="Запрошенный файл не найден в сессии")

    file_data = files[original_filename]
    file_content = file_data.get("file_content")
    if not file_content:
        logger.warning(f"No content found for file: {original_filename}")
        raise HTTPException(status_code=400, detail="Отсутствует содержимое PDF файла")

    try:
        logger.debug(f"Attempting to split PDF with pages: {pages}")
        output_stream = split_pdf(file_content, pages)
        logger.info(f"Successfully split PDF: {original_filename}")

        return StreamingResponse(
            output_stream,
            headers={"Content-Disposition": f"attachment; filename={quote(output_name)}"},
            media_type="application/pdf",
        )
    except HTTPException as he:
        logger.error(f"HTTP error during PDF split: {str(he)}")
        raise he
    except Exception as e:
        logger.error(f"Error splitting PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Внутренняя ошибка сервера: {str(e)}")


@app.get("/merge", response_class=HTMLResponse)
async def merge_page(request: Request):
    logger.info("Handling request for merge page")
    try:
        response = templates.TemplateResponse(
            "merge.html",
            {
                "request": request,
                "session": request.state.session,
            },
        )
        logger.info("Successfully rendered merge page")
        return response
    except Exception as e:
        logger.error(f"Error rendering merge page: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/merge-pdfs")
async def merge_pdfs_page(request: Request, filenames: list = Form(...), output_name: str = Form("merged.pdf")):
    logger.info(f"Starting PDF merge operation for files: {filenames}")
    try:
        session_files = request.state.session.get("files", [])
        logger.debug(f"Found {len(session_files)} files in session")

        merged_bytes = merge_pdfs(filenames, session_files)
        logger.info(f"Successfully merged {len(filenames)} PDFs into {output_name}")

        return StreamingResponse(
            merged_bytes,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={output_name}"},
        )
    except Exception as e:
        logger.error(f"Error merging PDFs: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/convert", response_class=HTMLResponse)
async def convert_page(request: Request):
    logger.info("Handling request for convert page")
    try:
        response = templates.TemplateResponse(
            "convert.html",
            {
                "request": request,
                "session": request.state.session,
            },
        )
        logger.info("Successfully rendered convert page")
        return response
    except Exception as e:
        logger.error(f"Error rendering convert page: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/convert-pdf-to-jpg")
async def convert_pdf_to_jpg(
        request: Request,
        filenames: list = Form(...),
        dpi: int = Form(300),
        output_name: str = Form("converted"),
):
    logger.info(f"Starting PDF to JPG conversion for files: {filenames}, DPI: {dpi}")
    try:
        requested_files = get_files_from_session(request, filenames)
        logger.debug(f"Found {len(requested_files)} requested files in session")

        individual_archives = []
        for filename, file_content in requested_files.items():
            logger.debug(f"Processing file: {filename}")
            individual_archive = convert_and_pack(filename, file_content, dpi)
            individual_archives.append(individual_archive)

        combined_archive = combine_archives(individual_archives)
        logger.info(f"Successfully converted and packed {len(filenames)} PDFs to JPG")

        return StreamingResponse(
            combined_archive,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={output_name}.zip"},
        )
    except Exception as e:
        logger.error(f"Error converting PDF to JPG: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/rotate", response_class=HTMLResponse)
async def rotate_page(request: Request):
    logger.info("Handling request for rotate page")
    try:
        response = templates.TemplateResponse(
            "rotate.html",
            {"request": request, "session": request.state.session},
        )
        logger.info("Successfully rendered rotate page")
        return response
    except Exception as e:
        logger.error(f"Error rendering rotate page: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


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
    logger.info(f"Starting PDF rotation for file: {original_filename}, pages_and_angles: {pages_and_angles}")

    if not pages_and_angles or pages_and_angles.strip() == "":
        logger.warning("No pages_and_angles provided for rotation")
        raise HTTPException(status_code=400, detail="Необходимо выбрать страницы и установить углы поворота.")

    try:
        pages_and_angles_dict = json.loads(pages_and_angles)
        logger.debug(f"Parsed rotation data: {pages_and_angles_dict}")
    except json.JSONDecodeError:
        logger.error("Invalid JSON format for pages_and_angles")
        raise HTTPException(status_code=400, detail="Неправильный формат JSON")

    files = request.state.session.get("files", {})
    logger.debug(f"Session contains {len(files)} files")

    if original_filename not in files:
        logger.warning(f"File not found in session: {original_filename}")
        raise HTTPException(status_code=400, detail="Запрошенный файл не найден в сессии")

    file_data = files[original_filename]
    file_content = file_data.get("file_content")
    if not file_content:
        logger.warning(f"No content found for file: {original_filename}")
        raise HTTPException(status_code=400, detail="Отсутствует содержимое PDF файла")

    try:
        rotations = [(int(page), angle) for page, angle in pages_and_angles_dict.items() if angle != 0]
        logger.debug(f"Prepared rotations: {rotations}")

        rotated_pdf = rotate_pages_in_pdf(file_content, rotations)
        logger.info(f"Successfully rotated PDF: {original_filename}")

        return StreamingResponse(
            rotated_pdf,
            headers={"Content-Disposition": f"attachment; filename={output_name}"},
            media_type="application/pdf",
        )
    except Exception as e:
        logger.error(f"Error rotating PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))