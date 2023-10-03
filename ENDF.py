import os
import configparser
import traceback
import zipfile
from ENDFParser import ENDFTape, NaNException
from DB import DBConnection

dats = []
zips = []

config = configparser.ConfigParser()
config.read('ENDF.properties')
endf_library = config.get("endf", "library_dir")

conn = DBConnection.getConnection()

print("Searching library directory: %s" % (endf_library))
try:
    for root, dirs, files in os.walk(endf_library):
        for name in files:
            ext = os.path.splitext(name)[1]
            if ext.lower()==".zip":
                zips.append(os.path.join(root,name))
            elif ext.lower()==".dat" or ext.lower()==".txt":
                dats.append(os.path.join(root,name))

except Exception as error:
    print("Error while scanning ENDF Library files: %s" % (endf_library))
    print(type(error))
    print(error)
    traceback.print_exc()

print("Found data files: %d\tzip files: %d" % (len(dats),len(zips)))
try:
    for dat_file in dats:
#        break # skip these to test zip parsing
        filename = dat_file.split(os.sep)[-1]
        rel_path = dat_file.replace(endf_library,'').replace(os.sep+filename,'')
        res = conn.execute("SELECT id from Files where name = %s and path = %s and zip_file is null",[filename,rel_path])
        if res:
            file_key = res[0][0]
        else:
            file_key = DBConnection.getNextId()
            conn.execute("INSERT INTO Files (id,name,path) VALUES(%s,%s,%s)",[file_key,filename,rel_path])

        print("Parsing file: %s at %s" % (filename,rel_path))
        tape = ENDFTape(dat_file)
        tape.parseTape()
        tape.setFileKey(file_key)
        for mat in tape.getMaterials():
            try:
                mat.persist()
                conn.commit()
#            except NaNException:
            except(Exception) as error:
                conn.rollback()
                print(str(error))
                traceback.print_exc()
                conn.execute("UPDATE Files set comment=%s where id=%s", ["Persist: "+str(error), file_key])
                conn.commit()
        #print("Inserted %d rows from file %s" % (len(data),dat_file))
    
    for zip_file in zips:
        filename = zip_file.split(os.sep)[-1]
        rel_path = zip_file.replace(endf_library,'').replace(os.sep+filename,'')
        archive = zipfile.ZipFile(zip_file, 'r')
        for dat_file in archive.namelist():
            res = conn.execute("SELECT id from Files where name = %s and path = %s and zip_file = %s",[dat_file,rel_path, filename])
            if res:
                file_key = res[0][0]
            else: 
                file_key = DBConnection.getNextId()           
                conn.execute("INSERT INTO Files (id,name,path,zip_file) VALUES(%s,%s,%s,%s)",[file_key,dat_file,rel_path, filename])
                conn.commit()

            print("Parsing file: %s in zip %s at %s" % (dat_file,zip_file,rel_path))
            tape = ENDFTape(dat_file,archive)
            try:
                tape.parseTape()
            except(Exception) as error:
                conn.execute("UPDATE Files set comment=%s where id=%s", ["Parse: "+str(error), file_key])
                conn.commit()
                continue
            tape.setFileKey(file_key)
            for mat in tape.getMaterials():
                try:
                    mat.persist()
                    conn.commit()
#                except NaNException:
                except Exception as error:
                    conn.rollback()
                    conn.execute("UPDATE Files set comment=%s where id=%s", ["Persist: "+str(error), file_key])
                    conn.commit()

except Exception as error:
    print(type(error))
    print(error)
    traceback.print_exc()
    if conn:
        conn.rollback()
finally:
    conn.close()