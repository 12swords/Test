from pdfminer.pdfinterp import PDFResourceManager, process_pdf
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from io import StringIO
from io import open

pdf_path = "test.pdf"
txt_path = "test.txt"
def readPDF(pdfFile):
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, laparams=laparams)

    process_pdf(rsrcmgr, device, pdfFile)
    device.close()

    content = retstr.getvalue()
    retstr.close()
    return content


def saveTxt(txt):
    with open(txt_path, "w",encoding='utf-8') as f:
        f.write(txt)


txt = readPDF(open(pdf_path, 'rb'))
saveTxt(txt)