import os
import mysql.connector
import configparser
import traceback
import zipfile
from ENDFParser import ENDFTape, NaNException

def parseZipFile(zip_file,file_id,archive):
    print("Parsing zip file: %s in archive: %s" % (zip_file,archive))
    data = []
    with archive.open(zip_file, "r") as dat:
        for line in dat:
            content = line[:66]
            MAT = int(line[66:70])
            MF = int(line[70:72])
            MT = int(line[72:75])
            NS = None
            try:
                NS = int(line[75:80])
            except ValueError:
                pass
            data.append([content,MAT,MF,MT,NS,file_id])
    return data

def parseDatFile(dat_file,file_id):
    data = []
    with open(dat_file, "r") as dat:
        for line in dat:
            content = line[:66]
            MAT = int(line[66:70])
            MF = int(line[70:72])
            MT = int(line[72:75])
            NS = None
            try:
                NS = int(line[75:80])
            except ValueError:
                pass
#            #print("MAT: %s MF: %s MT: %s NS: %s content: %s" %( MAT, MF, MT, NS, content))
#            #print("MAT: %s MF: %s MT: %s NS: %s content: %s" %( len(MAT), len(MF), len(MT), len(NS), len(content)))
            data.append([content,MAT,MF,MT,NS,file_id])
    return data

batch_size = 1000
#dat_file = "n_0125_1-H-1.dat"
data = []
#exts = {}
dats = []
zips = []

config = configparser.ConfigParser()
config.read('ENDF.properties')
endf_library = config.get("endf", "library_dir")
db_host=config.get("db", "db_host")
db_name=config.get("db", "db_name")
db_user=config.get("db", "user")
db_password=config.get("db", "password")

conn = None
cursor = None

print("Searching library directory: %s" % (endf_library))
try:
    for root, dirs, files in os.walk(endf_library):
        for name in files:
            ext = os.path.splitext(name)[1]
            if ext.lower()==".zip":
                zips.append(os.path.join(root,name))
            elif ext.lower()==".dat" or ext.lower()==".txt":
                dats.append(os.path.join(root,name))
            #cnt = 0
            #if ext in exts:
            #    cnt = exts.get(ext)
            #if ".lst" == ext:
            #    print(name)
            #exts[ext] = cnt+1
except Exception as error:
    print("Error while scanning ENDF Library files: %s" % (endf_library))
    print(type(error))
    print(error)
    traceback.print_exc()

print("Found data files: %d\tzip files: %d" % (len(dats),len(zips)))
try:
    conn = mysql.connector.connect(host=db_host,
                                   database=db_name,
                                   user=db_user,
                                   password=db_password)

    conn.autocommit = False
    cursor = conn.cursor()
    for dat_file in dats:
#        break # skip these to test zip parsing
        filename = dat_file.split(os.sep)[-1]
        rel_path = dat_file.replace(endf_library,'').replace(os.sep+filename,'')
        cursor.execute("SELECT id from Files where name = %s and path = %s and zip_file is null",[filename,rel_path])
        res = cursor.fetchone()
        if res is not None:
            file_key = res[0]
            print(file_key)
        else:

        #cursor.fetchall()
        #if cursor.rowcount > 0:
        #    print("Skipping file \"%s\" as it already exists in DB" % (dat_file))
        #    continue

            cursor.execute("INSERT INTO Files (name,path) VALUES(%s,%s)",[filename,rel_path])
            cursor.execute("SELECT id from Files where name = %s and path = %s and zip_file is null",[filename,rel_path])
            file_key = cursor.fetchone()[0]

        #data = parseDatFile(dat_file,file_id)

        #print("Inserting %d rows from file %s" % (len(data),dat_file))
        #for i in range(0,len(data),batch_size):
        #    cursor.executemany("INSERT INTO ENDF (content,MAT,MF,MT,NS,file_id) VALUES (%s,%s,%s,%s,%s,%s)", data[i:i+batch_size])
        print("Parsing file: %s at %s" % (filename,rel_path))
        tape = ENDFTape(dat_file)
        tape.parseTape()
        tape.setFileKey(file_key)
        for mat in tape.getMaterials():
            try:
                mat.persist(cursor)
                conn.commit()
#            except NaNException:
            except(Exception) as error:
                conn.rollback()
                print(str(error))
                cursor.execute("UPDATE Files set comment=%s where id=%s", ["Persist: "+str(error), file_key])
                conn.commit()
        #print("Inserted %d rows from file %s" % (len(data),dat_file))
    
    for zip_file in zips:
        data = []
        filename = zip_file.split(os.sep)[-1]
        rel_path = zip_file.replace(endf_library,'').replace(os.sep+filename,'')
        archive = zipfile.ZipFile(zip_file, 'r')
        for dat_file in archive.namelist():
            cursor.execute("SELECT id from Files where name = %s and path = %s and zip_file = %s",[dat_file,rel_path, filename])
            res = cursor.fetchone()
            if res is not None:
                file_key = res[0]
            else:            
            #cursor.fetchall()
            #if cursor.rowcount > 0:
            #    print("Skipping file \"%s\" as it already exists in DB" % (dat_file))
            #    continue
                cursor.execute("INSERT INTO Files (name,path,zip_file) VALUES(%s,%s,%s)",[dat_file,rel_path, filename])
                cursor.execute("SELECT id from Files where name = %s and path = %s and zip_file = %s",[dat_file,rel_path, filename])
                file_key = cursor.fetchone()[0]
            #print("Parsing file %s" % (dat_file))
            #data.extend(parseZipFile(dat_file,file_id,archive))

        #if data:
        #    print("Inserting %d rows from file %s" % (len(data),filename))
        #    for i in range(0,len(data),batch_size):
        #        cursor.executemany("INSERT INTO ENDF (content,MAT,MF,MT,NS,file_id) VALUES (%s,%s,%s,%s,%s,%s)", data[i:i+batch_size])
        #    conn.commit()
        #    print("Inserted %d rows from file %s" % (len(data),filename))
            print("Parsing file: %s in zip %s at %s" % (dat_file,zip_file,rel_path))
            tape = ENDFTape(dat_file,archive)
            try:
                tape.parseTape()
            except(Exception) as error:
                cursor.execute("UPDATE Files set comment=%s where id=%s", ["Parse: "+str(error), file_key])
                conn.commit()
                continue
            tape.setFileKey(file_key)
            for mat in tape.getMaterials():
                try:
                    mat.persist(cursor)
                    conn.commit()
#                except NaNException:
                except Exception as error:
                    conn.rollback()
                    cursor.execute("UPDATE Files set comment=%s where id=%s", ["Persist: "+str(error), file_key])
                    conn.commit()

except Exception as error:
  print(type(error))
  print(error)
  traceback.print_exc()
  if conn is not None:
    conn.rollback()
finally:
  if conn is not None and conn.is_connected():
    if cursor is not None:
      cursor.close()
    conn.close()

#except IOError:
#     print('Error While Opening the file!')  

#print(len(data))
#print(exts)
