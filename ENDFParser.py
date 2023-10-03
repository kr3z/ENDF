import math
from enum import Enum
import traceback
import io
import time
import numpy as np
import pandas as pd
from DB import DBConnection

BATCH_SIZE = 10000

class ENDFRecordType(Enum):
    TEXT = 1
    CONT = 2
    LIST = 3
    TAB1 = 4
    TAB2 = 5
    INTG = 6
    DIR =  7
    HEAD = 8
    SEND = 9
    FEND = 10
    MEND = 11
    TEND = 12
    TPID = 13
    UNDEF = 14

class NotImplementedYetException(Exception):
    pass

class NaNException(Exception):
    pass

class ENDFPersistable:
    def __init__(self):
        self.lib_key = None
        self.mat_key = None
    def getLibraryKey(self):
        return self.lib_key
    def setLibraryKey(self,key):
        self.lib_key = key
    def getMaterialKey(self):
        return self.mat_key
    def setMaterialKey(self,key):
        self.mat_key = key
    def getFileKey(self):
        return self.file_key
    def setFileKey(self,key):
        self.file_key = key

def parseFloat(floatStr):
    floatStr = "".join(floatStr.split()).replace('D', 'E')
    if floatStr.find('e')==-1 and floatStr.find('E')==-1:
        i_plus = floatStr.rfind('+')
        i_minus = floatStr.rfind('-')
        idx = (i_plus if i_plus>i_minus else i_minus)
        if idx > 0:
            efloatStr=floatStr[0:idx]+'E'+floatStr[idx:]
            floatStr=efloatStr
    return float(floatStr)

def parse_row(row: str,type_parsers: list) -> list:
    ret = []
    idx = 0
    while idx < 6:
        val_str = row[idx*11:(idx+1)*11].strip()
        ret.append(type_parsers[idx](val_str) if val_str else 0)
        idx += 1

    return ret

def parseCONT(row):
    return parse_row(row, [parseFloat,parseFloat,int,int,int,int])

def parseList(data,NC):
    C = []
    for row in data.to_list():
        C.extend(parse_row(row, [parseFloat,parseFloat,parseFloat,parseFloat,parseFloat,parseFloat]))
    return C[:NC]

def parseTAB1(NR,NP,interp_data,xy_data):
    NBTINT = []
    XY = []
    for row in interp_data.to_list():
        NBTINT.extend(parse_row(row,[int,int,int,int,int,int]))
    for row in xy_data.to_list():
        XY.extend(parse_row(row,[parseFloat,parseFloat,parseFloat,parseFloat,parseFloat,parseFloat]))

    NBT = NBTINT[0::2]
    INT = NBTINT[1::2]
    X = XY[0::2]
    Y = XY[1::2]

    return NBT[:NR], INT[:NR], X[:NP], Y[:NP]

class ENDFSection(ENDFPersistable):
    def __init__(self, data):
        self.timings = {"total": 0, "lib": 0, "mat": 0, "gi": 0, "dir": 0, "csinfo": 0, "interp": 0, "csdata": 0}
        self.material = int(data.iat[0,1])
        self.file     = int(data.iat[0,2])
        self.MT       = int(data.iat[0,3])

        self.mat_key = None
        self.lib_key = None
        self.file_key = None

        bad_MT = data[data['MT']!=self.MT]
        if len(bad_MT.index>0):
            print("MT should be %s but found other values: %s" % (self.MT, bad_MT))
            raise Exception("Bad MT values")
        
        self.parsed = True
        idx = 0
        
        try:
            if self.file == 1: # General Information
                # Descriptive Data and Directory
                if self.MT == 451: 
                    self.ZA, self.AWR, self.LRP, self.LFI, self.NLIB, self.NMOD = parseCONT(data.iat[idx,0])
                    idx += 1
                    self.ELIS, self.STA, self.LIS, self.LISO, _, self.NFOR = parseCONT(data.iat[idx,0])
                    idx += 1
                    self.AWI, self.EMAX, self.LREL, _, self.NSUB, self.NVER = parseCONT(data.iat[idx,0])
                    idx += 1
                    self.TEMP, _, self.LDRV, _, self.NWD, self.NXC = parseCONT(data.iat[idx,0])
                    idx += 1

                    self.desc = ""
                    self.section_data = []
                    for i in range(0,self.NWD):
                        self.desc = self.desc + '\n' + data.iat[idx+i,0]
                    idx += self.NWD
                    for i in range(0,self.NXC):
                        _, _, MF, MT, NC, MOD = parseCONT(data.iat[idx+i,0])
                        self.section_data.append([MF,MT,NC,MOD])
                    idx += self.NXC

                # 452: Number of Neutrons per Fission
                # 456: Number of Prompt Neutrons per Fission
                elif self.MT == 452 or self.MT == 456:
                    self.ZA, self.AWR, _, self.LNU, _, _ = parseCONT(data.iat[0,0])
                    if self.LNU == 1:
                        _, _, _, _, self.NC, _ = parseCONT(data.iat[1,0])
                        self.C = parseList(data.iloc[2:2+math.ceil(self.NC/6), 0],self.NC)
                    elif self.LNU == 2:
                        _, _, _, _, self.NR, self.NP = parseCONT(data.iat[1,0])
                        interp_lines = math.ceil(self.NR/3)
                        interp_data = data.iloc[2:2+interp_lines, 0]
                        xy_lines = math.ceil(self.NP/3)
                        xy_data = data.iloc[2+interp_lines:2+interp_lines+xy_lines, 0]
                        self.NBT, self.INT, self.X, self.Y = parseTAB1(self.NR,self.NP, interp_data, xy_data)
                    else:
                        raise Exception("Invalid LNU option for MF=%s MT=%s, LNU: %s" % (self.file,self.MT,self.LNU))
                    
                # Delayed Neutron Data
                elif self.MT == 455:
                    self.ZA, self.AWR, self.LDG, self.LNU, _, _ = parseCONT(data.iat[idx,0])
                    idx += 1
                    if self.LDG == 0:
                        _, _, _, _, self.NNF, _ = parseCONT(data.iat[idx,0])
                        idx += 1
                        self.decay_constant = parseList(data.iloc[idx:idx+math.ceil(self.NNF/6), 0],self.NNF)
                        idx += math.ceil(self.NNF/6)
                        _, _, _, _, self.NR, self.NP = parseCONT(data.iat[idx,0])
                        idx += 1
                        if self.LNU == 1:
                            self.Vd = parseList(data.iloc[idx:idx+1, 0],1)
                            idx += 1
                        elif self.LNU == 2:
                            interp_lines = math.ceil(self.NR/3)
                            interp_data = data.iloc[idx:idx+interp_lines, 0]
                            idx += interp_lines
                            xy_lines = math.ceil(self.NP/3)
                            xy_data = data.iloc[idx:idx+xy_lines, 0]
                            idx += xy_lines
                            self.NBT, self.INT, self.X, self.Y = parseTAB1(self.NR,self.NP,interp_data,xy_data)
                        else:
                            raise Exception("Invalid LNU value: LNU=%s" % (self.LNU))
                    elif self.LDG == 1:
                        #TODO: implement
                        raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))
                    else:
                        raise Exception("Invalid LDG value: LNU=%s" % (self.LDG))

                #  Components of Energy Release Due to Fission
                elif self.MT == 458:
                    self.ZA, self.AWR, _, self.LFC, _, self.NFC = parseCONT(data.iat[idx,0])
                    idx += 1
                    _, _, _, self.NPLY, self.N1, self.N2 = parseCONT(data.iat[idx,0])
                    idx += 1
                    self.C = parseList(data.iloc[idx:idx+math.ceil(self.N1/6), 0],self.N1)
                    idx += math.ceil(self.N1/6)

                    if self.LFC == 1:
                        self.EIFC = []
                        for _ in range (0,self.NFC):
                            _, _, LDRV, IFC, NR, NP = parseCONT(data.iat[idx,0])
                            idx += 1

                            interp_lines = math.ceil(NR/3)
                            interp_data = data.iloc[idx:idx+interp_lines, 0]
                            idx += interp_lines
                            xy_lines = math.ceil(NP/3)
                            xy_data = data.iloc[idx:idx+xy_lines, 0]
                            idx += xy_lines

                            NBT, INT, X, Y = parseTAB1(NR,NP,interp_data,xy_data)
                            self.EIFC.append([LDRV, IFC, NR, NP, NBT, INT, X, Y])

                #  Delayed Photon Data
                elif self.MT == 460:
                    self.ZA, self.AWR, self.LO, _,  self.NG, _ = parseCONT(data.iat[idx,0])
                    idx += 1
                    if self.LO == 1:
                        self.T = []
                        for _ in range (0,self.NG):
                            E, _, iNG, _, NR, NP = parseCONT(data.iat[idx,0])
                            idx += 1

                            interp_lines = math.ceil(NR/3)
                            interp_data = data.iloc[idx:idx+interp_lines, 0]
                            idx += interp_lines
                            xy_lines = math.ceil(NP/3)
                            xy_data = data.iloc[idx:idx+xy_lines, 0]
                            idx += xy_lines

                            NBT, INT, X, Y = parseTAB1(NR,NP,interp_data,xy_data)
                            self.T.append([E, iNG, NR, NP, NBT, INT, X, Y])
                    elif self.LO == 2:
                        _, _, _, _, _, self.NNF = parseCONT(data.iat[idx,0])
                        idx += 1
                        self.C = parseList(data.iloc[idx:idx+math.ceil(self.NNF/6), 0],self.NNF)
                        idx += math.ceil(self.NNF/6)

                    else:
                        raise Exception("Invalid LO value: LO=%s" % (self.LO))
                else:
                    raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))
            
            # Reaction Cross Sections
            elif self.file == 3:
                self.ZA, self.AWR, _, _, _, _ = parseCONT(data.iat[idx,0])
                idx += 1
                self.QM, self.QI, _, self.LR, self.NR, self.NP = parseCONT(data.iat[idx,0])
                idx += 1

                interp_lines = math.ceil(self.NR/3)
                interp_data = data.iloc[idx:idx+interp_lines, 0]
                idx += interp_lines
                xy_lines = math.ceil(self.NP/3)
                xy_data = data.iloc[idx:idx+xy_lines, 0]
                idx += xy_lines
                self.NBT, self.INT, self.X, self.Y = parseTAB1(self.NR,self.NP,interp_data,xy_data)

            else:
                raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))

        #TODO Parse other MTs
        except NotImplementedYetException:
            self.parsed = False 
            
    def persist(self):
        conn = DBConnection.getConnection()
        if not self.parsed:
            raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))
        t_begin = time.perf_counter()
        if self.file == 1 and self.MT == 451:
            #Persist Library
            t_lib_begin = time.perf_counter()
            #print("%s %s %s %s %s" % (self.NLIB.item(0),self.NSUB.item(0),self.NVER.item(0),self.LREL.item(0),self.NFOR.item(0)))
            #raise
            res = conn.execute("SELECT id FROM Library WHERE NLIB=%s and NSUB=%s and NVER=%s and LREL=%s and NFOR=%s",
                          [self.NLIB,self.NSUB,self.NVER,self.LREL,self.NFOR])
            if res:
                print("Library already exists for NLIB=%s, NSUB=%s, NVER=%s, LREL=%s, NFOR=%s" % (self.NLIB,self.NSUB,self.NVER,self.LREL,self.NFOR))
                self.lib_key = res[0][0]
            else:
                print("Persisting Library")
                IPART = str(self.NSUB)[0:-1] if len(str(self.NSUB))>1 else 0
                ITYPE = str(self.NSUB)[-1:]
                self.lib_key = DBConnection.getNextId()
                conn.execute("INSERT INTO Library(id,NLIB,NVER,LREL,NSUB,NFOR,IPART,ITYPE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                               [self.lib_key, self.NLIB, self.NVER, self.LREL, self.NSUB, self.NFOR, IPART, ITYPE])
            self.timings["lib"] = time.perf_counter() - t_lib_begin

            #Persist Material
            t_mat_begin = time.perf_counter()
            res = conn.execute("SELECT id from Material where MAT=%s and AWR=%s and LFI=%s and LIS=%s and LISO=%s and abs(ELIS-%s)<.05 and STA=%s",
                           [self.material,self.AWR,self.LFI,self.LIS, self.LISO, self.ELIS, self.STA])
            if res:
                self.mat_key = res[0][0]
            else:
                A = self.ZA % 1000
                Z = int(self.ZA/1000)
                print("Persisting material: MAT: %s AWR: %s LFI: %s LIS: %s LISO: %s ELIS: %s STA: %s" %
                               (self.material,self.AWR,self.LFI,self.LIS,self.LISO,self.ELIS,self.STA))
                self.mat_key = DBConnection.getNextId()
                conn.execute("INSERT INTO Material(id,MAT,Z,A,AWR,LFI,LIS,LISO,ELIS,STA) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                               [self.mat_key, self.material,Z,A,self.AWR,self.LFI,self.LIS,self.LISO,self.ELIS,self.STA])
            self.timings["mat"] = time.perf_counter() - t_mat_begin

            #Persist GeneralInfo (MT451)
            t_gi_begin = time.perf_counter()
            res = conn.execute("SELECT id from GeneralInfo WHERE material_key=%s and library_key=%s",
                           [self.mat_key, self.lib_key])
            if res:
                gi_key = res[0][0]
            else:
                gi_key = DBConnection.getNextId()
                conn.execute("INSERT INTO GeneralInfo(id,material_key,library_key,file_key,LRP,NMOD,AWI,EMAX,TEMP,LDRV,Description) VALUES (%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               [gi_key,self.mat_key,self.lib_key,self.file_key,self.LRP,self.NMOD,self.AWI,self.EMAX,self.TEMP,self.LDRV,self.desc])
            self.timings["gi"] = time.perf_counter() - t_gi_begin

            #Persist file directory
            t_dir_begin = time.perf_counter()
            res = conn.execute("SELECT 1 FROM Directory WHERE general_info_key=%s LIMIT 1",[gi_key])
            if not res:
                data = []
                dir_keys = DBConnection.get_ids(len(self.section_data))
                for i in range(0,len(self.section_data)):
                    dir_key = dir_keys[i]
                    entry = [dir_key,gi_key]
                    entry.extend(self.section_data[i])
                    data.append(entry)
                for i in range(0,len(data),BATCH_SIZE):
                    conn.executemany("INSERT INTO Directory(id,general_info_key,MF,MT,NC,Modification) VALUES(%s,%s,%s,%s,%s,%s)",
                                    data[i:i+BATCH_SIZE])
            self.timings["dir"] = time.perf_counter() - t_dir_begin

        elif self.file == 3:
            t_csinfo_begin = time.perf_counter()
            res = conn.execute("SELECT id FROM CrossSectionInfo WHERE MT=%s and material_key=%s and library_key=%s",
                           [self.MT, self.mat_key,self.lib_key])
            if res:
                cs_key = res[0][0]
            else:
                cs_key = DBConnection.getNextId()
                conn.execute("INSERT INTO CrossSectionInfo(id,MT,material_key,library_key,ZA,AWR,QM,QI,LR,NR,NP) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                               [cs_key,self.MT,self.mat_key,self.lib_key,self.ZA,self.AWR,self.QM,self.QI,self.LR,self.NR,self.NP])
            self.timings["csinfo"] = time.perf_counter() - t_csinfo_begin

            t_interp_begin = time.perf_counter()

            res = conn.execute("SELECT 1 FROM Interpolation WHERE info_key=%s and MT=%s and MF=%s limit 1",
                           [cs_key,self.MT,self.file])
            if not res:
                data = []
                i_keys = DBConnection.get_ids(self.NR)
                for i in range(0,self.NR):
                    i_key = i_keys[i]
                    data.append([i_key,cs_key,self.MT,self.file,self.NBT[i],self.INT[i]])
                for i in range(0,len(data),BATCH_SIZE):
                    conn.executemany("INSERT INTO Interpolation(id,info_key,MT,MF,NBT,InterpolationScheme) VALUES(%s,%s,%s,%s,%s,%s)",
                                       data[i:i+BATCH_SIZE])
            self.timings["interp"] = time.perf_counter() - t_interp_begin

            t_csdata_begin = time.perf_counter()
            res = conn.execute("SELECT 1 FROM CrossSectionData WHERE crosssectioninfo_key=%s LIMIT 1",
                           [cs_key])
            if not res:
                data = []
                csd_keys = DBConnection.get_ids(self.NP)
                for i in range(0,self.NP):
                    csd_key = csd_keys[i]
                    data.append([csd_key,cs_key,self.MT,self.X[i],self.Y[i]])
                    if (self.X[i] != self.X[i]) or (self.Y[i] != self.Y[i]):
                        raise NaNException
                for i in range(0,len(data),BATCH_SIZE):
                    conn.executemany("INSERT INTO CrossSectionData(id,crosssectioninfo_key,MT,Energy,CrossSection) VALUES(%s,%s,%s,%s,%s)",
                                       data[i:i+BATCH_SIZE])
            self.timings["csdata"] = time.perf_counter() - t_csdata_begin
        else:
            raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))
    
        self.timings["total"] = time.perf_counter() - t_begin

    def getTimings(self):
        return self.timings
    def getParsed(self):
        return self.parsed
    def getMT(self):
        return self.MT
    def getSectionData(self):
        return self.section_data
    def getFile(self):
        return self.file
    def getMaterial(self):
        return self.material
            


class ENDFFile(ENDFPersistable):
    def __init__(self,data):
        self.timings = {"total": 0, "lib": 0, "mat": 0, "gi": 0, "dir": 0, "csinfo": 0, "interp": 0, "csdata": 0}
        self.material = int(data.iat[0,1])
        self.file     = int(data.iat[0,2])

        self.mat_key = None
        self.lib_key = None

        bad_MF = data[data['MF']!=self.file]
        if len(bad_MF.index>0):
            print("MF should be %s but found other values: %s" % (self.file,bad_MF))
            raise Exception("Bad MF values")

        # Find SENDs in file
        SENDs = data.index[(data['MAT'] == self.material) & (data['MF']==self.file) & (data['MT']==0)]
        #print(SENDs)
        #print(len(data.index))
        #print(data)
        if SENDs[-1] != len(data.index)-1:
            raise Exception("Data after last SEND")
        
        #Split file into Sections
        section_start = 0
        self.sections = []
        for SEND in SENDs:
            section = data.iloc[section_start : SEND]
            section.index = range(len(section.index))
            section_start = SEND+1
            self.sections.append(ENDFSection(section))

    def persist(self):
        for section in self.sections:
            section.setFileKey(self.file_key)
            try:
                if self.mat_key is not None and self.lib_key is not None:
                    section.setMaterialKey(self.mat_key)
                    section.setLibraryKey(self.lib_key)
                section.persist()
                if self.mat_key is None or self.lib_key is None:
                    self.mat_key = section.getMaterialKey()
                    self.lib_key = section.getLibraryKey()
                sec_timings = section.getTimings()
                for timing in self.timings:
                    self.timings[timing] = self.timings.get(timing) + sec_timings.get(timing)

            except NotImplementedYetException:
                pass

    def getTimings(self):
        return self.timings
    def getSections(self):
        return self.sections
    def getSection(self,MT):
        return self.sections.get(MT)
    def getSectionData(self):
        if self.file == 1:
            return self.section_data
        else:
            return None
    def getFile(self):
        return self.file
    def getMaterial(self):
        return self.material


class ENDFMaterial(ENDFPersistable):
    def __init__(self, data):
        self.timings = {"total": 0, "lib": 0, "mat": 0, "gi": 0, "dir": 0, "csinfo": 0, "interp": 0, "csdata": 0}
        self.material = int(data.iat[0,1])
        self.mat_key = None
        self.lib_key = None
        bad_MAT = data[data['MAT']!=self.material]
        if len(bad_MAT.index>0):
            print("MAT should be %s but found other values: %s" % (self.material,bad_MAT))
            raise Exception("Bad MAT values")
        
        # Find FENDs in material
        FENDs = data.index[(data['MAT'] == self.material) & (data['MF']==0) & (data['MT']==0)]
        #print(FENDs)
        #print(len(data.index))
        #print(data)
        if FENDs[-1] != len(data.index)-1:
            raise Exception("Data after last FEND")
        
        #Split MAT into Files
        file_start = 0
        self.files = []
        for FEND in FENDs:
            file = data.iloc[file_start : FEND]
            file.index = range(len(file.index))
            file_start = FEND+1
            self.files.append(ENDFFile(file))     
                
    def persist(self):
        for file in self.files:
            file.setFileKey(self.file_key)
            if self.mat_key is not None and self.lib_key is not None:
                file.setMaterialKey(self.mat_key)
                file.setLibraryKey(self.lib_key)
            file.persist()
            if self.mat_key is None or self.lib_key is None:
                self.mat_key = file.getMaterialKey()
                self.lib_key = file.getLibraryKey()
            file_timings = file.getTimings()
            for timing in self.timings:
                self.timings[timing] = self.timings.get(timing) + file_timings.get(timing)
        for timing in self.timings:
            if self.timings.get(timing) > .001:
                print(f"Persisted {timing} in {self.timings.get(timing):0.4f} seconds")

    def getFiles(self):
        return self.files
    def getMaterial(self):
        return self.material
        
        

class ENDFTape:
    def __init__(self, filename, archive = None):
        self.filename = filename
        self.archive = archive
        self.file_key = None
        self.zip = (archive is not None)

    def parseTape(self):
        file = None
        try:
            self.materials = []
            if self.zip:
                file = io.TextIOWrapper(self.archive.open(self.filename, "r"),encoding ='ISO-8859-1')
            else:
                file = open(self.filename, "r", encoding ='ISO-8859-1')
            data = pd.DataFrame(np.genfromtxt(file, dtype="U66,i2,i1,i2,i4", names=['content','MAT','MF','MT','NS'],
                delimiter=[66,4,2,3,5], comments=None))
            
            nRows = len(data.index)
            
            #Find TEND indexes
            TENDs = data.index[(data['MAT']==-1) & (data['MF']==0) & (data['MT']==0)].to_list()

            #Should only be one TEND per tape
            if len(TENDs)>1:
                raise Exception("Tape has more than one TEND: %s" % (TENDs))
            
            TEND_idx = TENDs[0]

            if TEND_idx!=nRows-1:
                raise Exception("TEND is not last row in tape")

            self.TPID = data.iloc[0]
            self.NTAPE = data.iat[0,1]

            tape = data.iloc[1:TEND_idx]
            tape.index = range(len(tape.index))

            # Find MENDs in tape
            MENDs = tape.index[(tape['MAT']==0) & (tape['MF']==0) & (tape['MT']==0)]
            if MENDs[-1] != len(tape.index)-1:
                raise Exception("Data after last MEND")
            
            #Split tape into MATs
            MAT_start = 0
            for MEND in MENDs:
                MAT = tape.iloc[MAT_start : MEND]
                MAT.index = range(len(MAT.index))
                MAT_start = MEND+1
                self.materials.append(ENDFMaterial(MAT))


        except IOError:
            print('Error While Opening File: %s' % (self.filename))  
        finally:
            if file is not None:
                file.close()

    def getFileKey(self):
        return self.file_key
    def setFileKey(self,key):
        self.file_key = key
        for mat in self.materials:
            mat.setFileKey(self.file_key)
    def getMaterials(self):
        return self.materials
    def isZip(self):
        return self.zip


#config = configparser.ConfigParser()
#config.read('db.properties')

#db_host=config.get("db", "db_host")
#db_name=config.get("db", "db_name")
#db_user=config.get("db", "user")
#db_password=config.get("db", "password")

#conn = None
#cursor = None

#tape = ENDFTape("n_9437_94-Pu-239.dat")
#tape = ENDFTape("n_9034_90-TH-230.dat")
#tape = ENDFTape("mendl2_all.dat")
#tape = ENDFTape("n_0125_1-H-1.dat")
#tape = ENDFTape("n_005-B-11_0528.dat")
#tape = ENDFTape("g_9234_92-U-234.dat")
#tape = ENDFTape("decay_0131_1-H-3.dat")
#tape = ENDFTape("IRDF82.SL")
#tape = ENDFTape("n_5613_56-Ba-133M.dat")
#tape.parseTape()

#line='           0                 0.0 1.0000000-5-1.234567+1 + 1.2 +2  '
#CONT = np.genfromtxt(StringIO(line), dtype="f8,f8,f8,f8,f8,f8", names=['C1', 'C2', 'L1', 'L2', 'N1', 'N2'],
#                delimiter=[11,11,11,11,11,11], autostrip=True, filling_values={0:0.0, 1:0.0, 2:0.0, 3:0.0, 4:0.0, 5:0.0},
#                converters={0:parseFloat, 1:parseFloat, 2:parseFloat, 3:parseFloat, 4:parseFloat, 5:parseFloat})
#print(CONT)

#for mat in tape.getMaterials():
#    for file in mat.getFiles():
#        for section in file.getSections():
#            print("mat: %s fmat: %s file: %s smat: %s sfile: %s MT: %s" % (mat.getMaterial(), file.getMaterial(), file.getFile(), section.getMaterial(), section.getFile(), section.getMT()))

#try:
#    conn = mysql.connector.connect(host=db_host,
#                                   database=db_name,
#                                   user=db_user,
#                                   password=db_password)

#    conn.autocommit = False
#    cursor = conn.cursor()                  
#    for mat in tape.getMaterials():
#        mat.persist(cursor)
#        conn.commit()
#except Exception as error:
#  print(type(error))
#  print(error)
#  traceback.print_exc()
#  if conn is not None:
#    conn.rollback()
#finally:
#  if conn is not None and conn.is_connected():
#    if cursor is not None:
#      cursor.close()
#    conn.close()
