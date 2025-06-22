import zipfile
from io import BytesIO
from typing import Dict

import pymupdf as fitz
from fastapi import HTTPException, Request
from PIL import Image
from PyPDF2 import PdfMerger, PdfReader, PdfWriter

from logger import get_logger

logger = get_logger(__name__)


def convert_pdf_to_images(pdf_bytes: bytes, quality: int = 50) -> list[bytes]:
    """
    Конвертирует страницы PDF в список байтов изображений JPEG.
    :param pdf_bytes: Байтовые данные PDF-файла
    :param quality: Уровень качества JPEG (0-100)
    :return: Список байтовых строк с изображениями
    """
    logger.info(f"Starting PDF to images conversion, quality: {quality}")
    try:
        pdf_document = fitz.open(stream=BytesIO(pdf_bytes))
        images = []
        logger.debug(f"Processing PDF with {len(pdf_document)} pages")

        for page_num in range(len(pdf_document)):
            logger.debug(f"Processing page {page_num + 1}")
            page = pdf_document.load_page(page_num)
            pixmap = page.get_pixmap()
            img = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)

            image_buffer = BytesIO()
            img.save(image_buffer, format="JPEG", quality=quality, optimize=True)
            images.append(image_buffer.getvalue())

        logger.info(f"Successfully converted {len(images)} pages to JPEG")
        return images
    except Exception as e:
        logger.error(f"Error converting PDF to images: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error converting PDF to images: {str(e)}")


def parse_page_ranges(pages: str) -> list[int]:
    """Парсит строку с диапазонами страниц в список номеров страниц"""
    logger.debug(f"Parsing page ranges: {pages}")
    page_ranges = []
    try:
        for part in pages.split(","):
            part = part.strip()
            if "-" in part:
                start, end = map(int, part.split("-"))
                page_ranges.extend(range(start - 1, end))  # -1 для 0-based индекса
            else:
                page_ranges.append(int(part) - 1)
        return sorted(list(set(page_ranges)))  # Удаляем дубликаты и сортируем
    except ValueError as e:
        logger.error(f"Invalid page range format: {pages} - {str(e)}")
        raise


def split_pdf(file_content: bytes, pages: str) -> BytesIO:
    """
    Разделяет PDF по заданным страницам
    :param file_content: Байтовое содержимое PDF
    :param pages: Строка с номерами страниц (например, "1-3,5,7-9")
    :return: BytesIO поток с результирующим PDF
    :raises HTTPException: При ошибках обработки
    """
    logger.info(f"Splitting PDF with pages: {pages}")
    try:
        page_numbers = parse_page_ranges(pages)
        logger.debug(f"Parsed page numbers: {page_numbers}")
    except ValueError:
        logger.error(f"Invalid page range format: {pages}")
        raise HTTPException(status_code=400, detail="Некорректный формат номеров страниц")

    try:
        pdf_reader = PdfReader(BytesIO(file_content))
        logger.debug(f"PDF has {len(pdf_reader.pages)} pages total")
        pdf_writer = PdfWriter()

        for page_num in page_numbers:
            if 0 <= page_num < len(pdf_reader.pages):
                pdf_writer.add_page(pdf_reader.pages[page_num])
            else:
                logger.warning(f"Page {page_num + 1} out of range (max {len(pdf_reader.pages)})")
                raise HTTPException(status_code=400, detail=f"Страница {page_num + 1} не существует в документе")

        output_stream = BytesIO()
        pdf_writer.write(output_stream)
        output_stream.seek(0)
        logger.info(f"Successfully split PDF into {len(page_numbers)} pages")
        return output_stream
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error splitting PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при обработке PDF: {str(e)}")


def merge_pdfs(filenames: list[str], session_files: dict) -> BytesIO:
    """
    Объединяет несколько PDF из сессии в один
    :param filenames: Список имен файлов для объединения
    :param session_files: Словарь файлов из сессии {filename: file_data}
    :return: BytesIO поток с объединенным PDF
    :raises HTTPException: Если файлы не найдены
    """
    logger.info(f"Merging PDFs: {filenames}")
    merger = PdfMerger()
    output = BytesIO()

    try:
        # Проверяем наличие всех файлов
        missing_files = [f for f in filenames if f not in session_files]
        if missing_files:
            logger.warning(f"Missing files: {missing_files}")
            raise HTTPException(status_code=404, detail=f"Файлы не найдены: {', '.join(missing_files)}")

        # Добавляем файлы в merger
        for filename in filenames:
            logger.debug(f"Adding file to merge: {filename}")
            file_data = session_files[filename]
            merger.append(BytesIO(file_data["file_content"]))

        merger.write(output)
        output.seek(0)
        logger.info(f"Successfully merged {len(filenames)} PDFs")
        return output
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error merging PDFs: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при объединении PDF: {str(e)}")
    finally:
        merger.close()
        logger.debug("PDF merger closed")


def get_files_from_session(request: Request, filenames: list[str]) -> Dict[str, bytes]:
    """
    Получает файлы из сессии по именам
    :param request: FastAPI Request объект
    :param filenames: Список имен файлов
    :return: Словарь {имя_файла: содержимое_файла}
    :raises HTTPException: Если файлы не найдены
    """
    logger.info(f"Getting files from session: {filenames}")
    session_files = request.state.session.get("files", {})
    logger.debug(f"Session contains {len(session_files)} files")
    result = {}

    try:
        for filename in filenames:
            if filename not in session_files:
                logger.warning(f"File not found in session: {filename}")
                raise HTTPException(status_code=404, detail=f"Файл '{filename}' не найден в сессии")

            file_content = session_files[filename].get("file_content")
            if not file_content:
                logger.warning(f"No content for file: {filename}")
                raise HTTPException(status_code=400, detail=f"Отсутствует содержимое файла '{filename}'")

            result[filename] = file_content
            logger.debug(f"Added file to result: {filename}")

        logger.info(f"Successfully retrieved {len(result)} files from session")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting files from session: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


def convert_pdf_to_jpeg(file_content: bytes, dpi: int = 300) -> list[Image.Image]:
    """
    Конвертирует PDF в список изображений JPEG
    :param file_content: Байтовое содержимое PDF
    :param dpi: Разрешение изображений
    :return: Список объектов PIL.Image
    """
    logger.info(f"Converting PDF to JPEG with DPI: {dpi}")
    try:
        doc = fitz.open(stream=BytesIO(file_content))
        images = []
        logger.debug(f"PDF has {doc.page_count} pages")

        for page_num in range(doc.page_count):
            logger.debug(f"Converting page {page_num + 1}")
            page = doc.load_page(page_num)
            pixmap = page.get_pixmap(dpi=dpi)
            img = Image.frombytes("RGB", [pixmap.width, pixmap.height], pixmap.samples)
            images.append(img)

        logger.info(f"Successfully converted {len(images)} pages to JPEG")
        return images
    except Exception as e:
        logger.error(f"Error converting PDF to JPEG: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error converting PDF to JPEG: {str(e)}")


def pack_images_into_zip(images: list[Image.Image], prefix: str) -> BytesIO:
    """
    Упаковывает изображения в ZIP-архив
    :param images: Список изображений PIL.Image
    :param prefix: Префикс для имен файлов
    :return: BytesIO поток с ZIP-архивом
    """
    logger.info(f"Packing {len(images)} images into ZIP with prefix: {prefix}")
    try:
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            for idx, img in enumerate(images, start=1):
                img_bytes = BytesIO()
                img.save(img_bytes, format="JPEG", quality=85)
                filename = f"{prefix}_page_{idx}.jpg"
                zf.writestr(filename, img_bytes.getvalue())
                logger.debug(f"Added image to ZIP: {filename}")

        buffer.seek(0)
        logger.info(f"Successfully created ZIP archive with {len(images)} images")
        return buffer
    except Exception as e:
        logger.error(f"Error packing images to ZIP: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error packing images to ZIP: {str(e)}")


def convert_and_pack(filename: str, file_content: bytes, dpi: int = 300) -> BytesIO:
    """
    Конвертирует PDF в JPEG и упаковывает в ZIP
    :param filename: Имя исходного файла
    :param file_content: Байтовое содержимое PDF
    :param dpi: Разрешение изображений
    :return: BytesIO поток с ZIP-архивом
    """
    logger.info(f"Converting and packing PDF: {filename}, DPI: {dpi}")
    try:
        images = convert_pdf_to_jpeg(file_content, dpi)
        prefix = filename.rsplit(".", 1)[0]  # Удаляем расширение
        logger.debug(f"Using prefix for ZIP: {prefix}")
        return pack_images_into_zip(images, prefix)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in convert_and_pack: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing PDF: {str(e)}")


def combine_archives(individual_archives: list[BytesIO]) -> BytesIO:
    """
    Объединяет несколько ZIP-архивов в один
    :param individual_archives: Список BytesIO потоков с ZIP-архивами
    :return: BytesIO поток с объединенным ZIP-архивом
    """
    logger.info(f"Combining {len(individual_archives)} ZIP archives")
    try:
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, "w") as zf:
            for idx, archive in enumerate(individual_archives, start=1):
                archive.seek(0)
                with zipfile.ZipFile(archive) as src_zip:
                    for name in src_zip.namelist():
                        new_name = f"file_{idx}/{name}"
                        zf.writestr(new_name, src_zip.read(name))
                        logger.debug(f"Added file to combined ZIP: {new_name}")

        buffer.seek(0)
        logger.info(f"Successfully combined {len(individual_archives)} archives")
        return buffer
    except Exception as e:
        logger.error(f"Error combining archives: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error combining archives: {str(e)}")


def rotate_pages_in_pdf(file_content: bytes, rotations: list[tuple[int, int]]) -> BytesIO:
    """
    Поворачивает указанные страницы PDF на указанные углы
    :param file_content: Байтовое содержимое PDF
    :param rotations: Список кортежей (номер_страницы, угол_поворота)
                     где номер_страницы начинается с 1 (1-based)
                     угол_поворота может быть 0, 90, 180 или 270 градусов
    :return: BytesIO поток с результатом
    """
    logger.info(f"Rotating PDF pages: {rotations}")
    try:
        reader = PdfReader(BytesIO(file_content))
        writer = PdfWriter()
        logger.debug(f"PDF has {len(reader.pages)} pages")

        # Создаем словарь для быстрого доступа к углам поворота
        rotations_dict = {page_num: angle for page_num, angle in rotations}
        logger.debug(f"Rotations dictionary: {rotations_dict}")

        rotated_pages = 0
        for i, page in enumerate(reader.pages):
            page_num = i + 1  # Преобразуем в 1-based индекс
            if page_num in rotations_dict:
                angle = rotations_dict[page_num]
                if angle != 0:  # Поворачиваем только если угол не 0
                    page.rotate(angle)
                    rotated_pages += 1
                    logger.debug(f"Rotated page {page_num} by {angle} degrees")

            writer.add_page(page)

        output_stream = BytesIO()
        writer.write(output_stream)
        output_stream.seek(0)
        logger.info(f"Successfully rotated {rotated_pages} pages")
        return output_stream
    except Exception as e:
        logger.error(f"Error rotating PDF pages: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при повороте страниц: {str(e)}")