import os
import shutil
import zipfile
import glob

zipDir = "./inputFiles"
destDir = "./extract"


zipfiles = glob.glob(zipDir + '/*.zip')

for inputFile in zipfiles:
    print "Processing " + inputFile
    with zipfile.ZipFile(inputFile) as zipToExtract:
        for member in zipToExtract.namelist():
            filename = os.path.basename(member)
            print filename 
            outputFileName = os.path.splitext(os.path.basename(inputFile))[0] + ".csv"

            source = zipToExtract.open(member)
            target = file(os.path.join(destDir, outputFileName), "wb")
            with source, target:
                shutil.copyfileobj(source, target)

