import zipfile
import xml.etree.ElementTree as ET
import sys

def read_docx(path):
    with zipfile.ZipFile(path) as docx:
        tree = ET.XML(docx.read('word/document.xml'))
    namespaces = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
    texts = [node.text for node in tree.iterfind('.//w:t', namespaces) if node.text]
    return '\n'.join(texts)

print(read_docx(sys.argv[1]))
