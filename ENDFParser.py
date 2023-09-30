import re
import math
from enum import Enum
import traceback
import io
import time
import numpy
from DB import DBConnection

BATCH_SIZE = 1000

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

class ENDException(Exception):
    "Exception used to signal TEND/MEND/FEND/SEND found"
    def __init__(self,type):
        self.type = type
    def getType(self):
        return self.type

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

class ENDFRecord:
    def __init__(self, data):
        #print(data)
        try:
            self.content = data[:66]
            self.MAT = int(data[66:70])
            self.MF = int(data[70:72])
            self.MT = int(data[72:75])
        except:
            print("Failed to parse data: %s" % (data))
            raise
        # NS is optional
        self.NS = None
        try:
            self.NS = int(data[75:80])
        except ValueError:
            pass

        #set type to none for now
        self.type = None

        # check if some type of END record
        try:
            C1, C2, L1, L2, N1, N2 = self.parseCONT()
            if C1 == 0.0 and C2 == 0.0 and L1 == 0 and L2 == 0 and N1 == 0 and N2 == 0:
                if self.MT==0 and self.MF==0 and self.MAT==-1:
                    self.type = ENDFRecordType.TEND
                elif self.MT == 0 and self.MF == 0 and self.MAT == 0:
                    self.type = ENDFRecordType.MEND
                elif self.MT == 0 and self.MF == 0 and self.MAT > 0 and self.NS != 99999:
                    self.type = ENDFRecordType.FEND
                elif (self.MT == 0 and self.MF > 0 and self.MAT > 0) or self.NS == 99999:
                    self.type = ENDFRecordType.SEND
                #print(self.type)
        except (AttributeError,ValueError,TypeError,OverflowError) as error:
            # Not an END record
            #print(error)
            pass

    re_float = re.compile("^(?P<sign>[ +-])? *(?P<significand>[0-9.]+)[ D]*(?P<expsign>[+-])? ?(?P<exp>[0-9]+)? *$")
    def parseFloat(floatStr):
        try:
            return float(floatStr)
        except ValueError:
            match = ENDFRecord.re_float.match(floatStr)
            fl = float(match.group('significand'))
            if match.group('sign')=='-':
                fl = fl * -1.0
            exp = int(match.group('exp'))
            if exp>0:
                if match.group('expsign')=='-':
                    exp = exp*-1
                fl = fl * 10**exp
            return round(fl,9)
        
    def parseFloat_new(floatStr):
        floatStr = "".join(floatStr.split())
        if floatStr.find('e')==-1 and floatStr.find('E')==-1:
            i_plus = floatStr.rfind('+')
            i_minus = floatStr.rfind('-')
            idx = (i_plus if i_plus>i_minus else i_minus)
            efloatStr=floatStr[0:idx]+'E'+floatStr[idx:]
            floatStr=efloatStr
        return round(float(floatStr),9)

    def setTPID(self):
        if not(self.MT==0 and self.MF==0):
            raise Exception("Not a valid TPID record! MAT: %s MF: %s MT: %s" % (self.MAT, self.MF, self.MT))
        self.type = ENDFRecordType.TPID

    def getContent(self):
        return self.content
    def getMAT(self):
        return self.MAT
    def getMF(self):
        return self.MF
    def getMT(self):
        return self.MT
    def getNS(self):
        return self.NS
    def getType(self):
        return self.type

    def isTEND(self):
        return self.type == ENDFRecordType.TEND
    def isMEND(self):
        return self.type == ENDFRecordType.MEND
    def isFEND(self):
        return self.type == ENDFRecordType.FEND
    def isSEND(self):
        return self.type == ENDFRecordType.SEND

    def parseCONT(self):
        C1_str = self.content[:11]
        C2_str = self.content[11:22]
        L1_str = self.content[22:33]
        L2_str = self.content[33:44]
        N1_str = self.content[44:55]
        N2_str = self.content[55:66]

        C1 = 0.0 if C1_str.strip()=='' else ENDFRecord.parseFloat(C1_str)
        C2 = 0.0 if C2_str.strip()=='' else ENDFRecord.parseFloat(C2_str)
        L1 = 0 if L1_str.strip()=='' else int(L1_str)
        L2 = 0 if L2_str.strip()=='' else int(L2_str)
        N1 = 0 if N1_str.strip()=='' else int(N1_str)
        N2 = 0 if N2_str.strip()=='' else int(N2_str)
        return C1, C2, L1, L2, N1, N2
    
    def parseTAB1(self,NR,NP,file):
        NBT = []
        INT = []
        X = []
        Y = []
        recs_read = []
        #interp_lines = int(NR/3)+1
        #xy_lines = int(NP/3)+1
        interp_lines = math.ceil(NR/3)
        xy_lines = math.ceil(NP/3)
        for _ in range(0,interp_lines):
            rec = ENDFRecord(file.readline())
            recs_read.append(rec)
            NBT1_str = rec.content[:11]
            INT1_str = rec.content[11:22]
            NBT2_str = rec.content[22:33]
            INT2_str = rec.content[33:44]
            NBT3_str = rec.content[44:55]
            INT3_str = rec.content[55:66]
            NBT.append(0 if NBT1_str.strip()=='' else int(NBT1_str))
            NBT.append(0 if NBT2_str.strip()=='' else int(NBT2_str))
            NBT.append(0 if NBT3_str.strip()=='' else int(NBT3_str))
            INT.append(0 if INT1_str.strip()=='' else int(INT1_str))
            INT.append(0 if INT2_str.strip()=='' else int(INT2_str))
            INT.append(0 if INT3_str.strip()=='' else int(INT3_str))
        for _ in range(0,xy_lines):
            rec = ENDFRecord(file.readline())
            recs_read.append(rec)
            X1_str = rec.content[:11]
            Y1_str = rec.content[11:22]
            X2_str = rec.content[22:33]
            Y2_str = rec.content[33:44]
            X3_str = rec.content[44:55]
            Y3_str = rec.content[55:66]
            X.append(0.0 if X1_str.strip()=='' else ENDFRecord.parseFloat(X1_str))
            X.append(0.0 if X2_str.strip()=='' else ENDFRecord.parseFloat(X2_str))
            X.append(0.0 if X3_str.strip()=='' else ENDFRecord.parseFloat(X3_str))
            Y.append(0.0 if Y1_str.strip()=='' else ENDFRecord.parseFloat(Y1_str))
            Y.append(0.0 if Y2_str.strip()=='' else ENDFRecord.parseFloat(Y2_str))
            Y.append(0.0 if Y3_str.strip()=='' else ENDFRecord.parseFloat(Y3_str))
        return NBT[:NR], INT[:NR], X[:NP], Y[:NP], recs_read

    def parseList(self,file,NC):
        C = []
        recs_read = []
        lines = math.ceil(NC/6)
        for _ in range(0,lines):
            rec = ENDFRecord(file.readline())
            recs_read.append(rec)
            C1_str = rec.content[:11]
            C2_str = rec.content[11:22]
            C3_str = rec.content[22:33]
            C4_str = rec.content[33:44]
            C5_str = rec.content[44:55]
            C6_str = rec.content[55:66]
            C.append(0.0 if C1_str.strip()=='' else ENDFRecord.parseFloat(C1_str))
            C.append(0.0 if C2_str.strip()=='' else ENDFRecord.parseFloat(C2_str))
            C.append(0.0 if C3_str.strip()=='' else ENDFRecord.parseFloat(C3_str))
            C.append(0.0 if C4_str.strip()=='' else ENDFRecord.parseFloat(C4_str))
            C.append(0.0 if C5_str.strip()=='' else ENDFRecord.parseFloat(C5_str))
            C.append(0.0 if C6_str.strip()=='' else ENDFRecord.parseFloat(C6_str))
        return C[:NC], recs_read

class ENDFSection(ENDFPersistable):
    def __init__(self, head, file):
        if head.isFEND():
            raise ENDException(ENDFRecordType.FEND)
        self.MT = head.getMT()
        self.file = head.getMF()
        self.material = head.getMAT()
        self.mat_key = None
        self.lib_key = None
        self.file_key = None
        self.parsed = True
        self.records = [head]
        self.timings = {"total": 0, "lib": 0, "mat": 0, "gi": 0, "dir": 0, "csinfo": 0, "interp": 0, "csdata": 0}
#        print("%s %s" % (head.content,head.MT))
        try:
            if self.file == 1:
                if self.MT == 451:
                    self.ZA, self.AWR, self.LRP, self.LFI, self.NLIB, self.NMOD = head.parseCONT()
                    rec = ENDFRecord(file.readline())
                    self.records.append(rec)
                    self.ELIS, self.STA, self.LIS, self.LISO, _, self.NFOR = rec.parseCONT()

                    rec = ENDFRecord(file.readline())
                    self.records.append(rec)
                    self.AWI, self.EMAX, self.LREL, _, self.NSUB, self.NVER = rec.parseCONT()

                    rec = ENDFRecord(file.readline())
                    self.records.append(rec)
                    self.TEMP, _, self.LDRV, _, self.NWD, self.NXC = rec.parseCONT()

                    self.desc = ""
                    self.section_data = []
                    for i in range(0,self.NWD):
                        rec = ENDFRecord(file.readline())
                        self.records.append(rec)
                        self.desc = self.desc + rec.getContent() + '\n'
                    for i in range(0,self.NXC):
                        rec = ENDFRecord(file.readline())
                        if rec.isSEND():
                            return
                        self.records.append(rec)
                        _, _, MF, MT, NC, MOD = rec.parseCONT()
                        self.section_data.append([MF,MT,NC,MOD])
                        
                    #SEND = ENDFRecord(file.readline())
                    #if not SEND.isSEND():
                    #    print("content: %s MAT: %s MF: %s MT: %s NS: %s" % (SEND.getContent(), SEND.getMAT(), SEND.getMF(), SEND.getMT(), SEND.getNS()))
                    #    raise Exception("ERROR: Record exists where SEND should be")

                elif self.MT == 452 or self.MT == 456:
                    self.ZA, self.AWR, _, self.LNU, _, _ = head.parseCONT()
                    rec = ENDFRecord(file.readline())
                    self.records.append(rec)
                    if self.LNU == 1:
                        _, _, _, _, self.NC, _ = rec.parseCONT()
                        self.C, recs = rec.parseList(file,self.NC)
                    elif self.LNU == 2:
                        _, _, _, _, self.NR, self.NP = rec.parseCONT()
                        self.NBT, self.INT, self.X, self.Y, recs = rec.parseTAB1(self.NR,self.NP,file)
                    else:
                        raise Exception("Invalid LNU option for MF=%s MT=%s, LNU: %s" % (self.file,self.MT,self.LNU))
                    self.records.extend(recs)
                elif self.MT == 455:
                    self.ZA, self.AWR, self.LDG, self.LNU, _, _ = head.parseCONT()
                    if self.LDG == 0:
                        rec = ENDFRecord(file.readline())
                        self.records.append(rec)
                        _, _, _, _, self.NNF, _ = rec.parseCONT()
                        self.decay_constant, recs = rec.parseList(file,self.NNF)
                        self.records.extend(recs)
                        rec = ENDFRecord(file.readline())
                        self.records.append(rec)
                        _, _, _, _, self.NR, self.NP = rec.parseCONT()
                        if self.LNU == 1:
                            self.Vd, recs = rec.parseList(file,1)
                        elif self.LNU == 2:
                            self.NBT, self.INT, self.X, self.Y, recs = rec.parseTAB1(self.NR,self.NP,file)
                        else:
                            raise Exception("Invalid LNU value: LNU=%s" % (self.LNU))
                        self.records.extend(recs)
                    elif self.LDG == 1:
                        raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))
                    else:
                        raise Exception("Invalid LDG value: LNU=%s" % (self.LDG))

                elif self.MT == 458:
                    self.ZA, self.AWR, _, self.LFC, _, self.NFC = head.parseCONT()
                    rec = ENDFRecord(file.readline())
                    self.records.append(rec)
                    _, _, _, self.NPLY, self.N1, self.N2 = rec.parseCONT()
                    self.C, recs = rec.parseList(file,self.N1)
                    self.records.extend(recs)
                    if self.LFC == 1:
                        self.EIFC = []
                        for _ in range (0,self.NFC):
                            rec = ENDFRecord(file.readline())
                            self.records.append(rec)
                            _, _, LDRV, IFC, NR, NP = rec.parseCONT()
                            NBT, INT, X, Y, recs = rec.parseTAB1(NR,NP,file)
                            self.EIFC.append([LDRV, IFC, NR, NP, NBT, INT, X, Y])
                            self.records.extend(recs)

                elif self.MT == 460:
                    self.ZA, self.AWR, self.LO, _,  self.NG, _ = head.parseCONT()
                    if self.LO == 1:
                        self.T = []
                        for _ in range (0,self.NG):
                            rec = ENDFRecord(file.readline())
                            self.records.append(rec)
                            E, _, iNG, _, NR, NP = rec.parseCONT()
                            NBT, INT, X, Y, recs = rec.parseTAB1(NR,NP,file)
                            self.T.append([E, iNG, NR, NP, NBT, INT, X, Y])
                            self.records.extend(recs)
                    elif self.LO == 2:
                        rec = ENDFRecord(file.readline())
                        self.records.append(rec)
                        _, _, _, _, _, self.NNF = rec.parseCONT()
                        self.C, recs = rec.parseList(file,self.NNF)
                        self.records.extend(recs)

                    else:
                        raise Exception("Invalid LO value: LO=%s" % (self.LO))

                else:
                    raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))
                SEND = ENDFRecord(file.readline())
                if not SEND.isSEND():
                    print("content: %s MAT: %s MF: %s MT: %s NS: %s" % (SEND.getContent(), SEND.getMAT(), SEND.getMF(), SEND.getMT(), SEND.getNS()))
                    raise Exception("ERROR: Record exists where SEND should be")

            elif self.file == 3:
                self.ZA, self.AWR, _, _, _, _ = head.parseCONT()
                rec = ENDFRecord(file.readline())
                self.records.append(rec)
                self.QM, self.QI, _, self.LR, self.NR, self.NP = rec.parseCONT()
                self.NBT, self.INT, self.X, self.Y, recs = rec.parseTAB1(self.NR,self.NP,file)
                self.records.extend(recs)
                
                SEND = ENDFRecord(file.readline())
                if not SEND.isSEND():
                    print("content: %s MAT: %s MF: %s MT: %s NS: %s" % (SEND.getContent(), SEND.getMAT(), SEND.getMF(), SEND.getMT(), SEND.getNS()))
                    raise Exception("ERROR: Record exists where SEND should be")

            else:
                raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))

        #TODO Parse other MTs
        except NotImplementedYetException:
            self.parsed = False 
            while(True):
                rec = ENDFRecord(file.readline())
                if rec.isSEND():
                    break
                self.records.append(rec)
        #print("Parsed Section: MAT=%s MF=%s MT=%s" % (self.material, self.file, self.MT))
            
    def persist(self):
        conn = DBConnection.getConnection()
        if not self.parsed:
            raise NotImplementedYetException("MF: %s MT: %s" % (self.file, self.MT))
        t_begin = time.perf_counter()
        if self.file == 1 and self.MT == 451:
            #Persist Library
            t_lib_begin = time.perf_counter()
            res = conn.execute("SELECT id FROM Library WHERE NLIB=%s and NSUB=%s and NVER=%s and LREL=%s and NFOR=%s",
                          [self.NLIB,self.NSUB,self.NVER,self.LREL,self.NFOR])
            if res:
                print("Library already exists for NLIB=%s, NSUB=%s, NVER=%s, LREL=%s, NFOR=%s" % (self.NLIB,self.NSUB,self.NVER,self.LREL,self.NFOR))
                self.lib_key = res[0][0]
            else:
                print("Persisting Library")
                IPART = str(self.NSUB)[0:-1]
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
                for i in range(0,len(self.section_data)):
                    dir_key = DBConnection.getNextId()
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
                for i in range(0,self.NR):
                    i_key = DBConnection.getNextId()
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
                for i in range(0,self.NP):
                    csd_key = DBConnection.getNextId()
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
    def __init__(self, head, file):
        #print("file head: MAT: %s MF: %s MT: %s" % (head.getMAT(),head.getMF(), head.getMT()))
        if head.isMEND():
            raise ENDException(ENDFRecordType.MEND)
        self.file = head.getMF()
        self.material = head.getMAT()
        self.mat_key = None
        self.lib_key = None
        self.file_key = None
        self.sections = {}
        self.timings = {"total": 0, "lib": 0, "mat": 0, "gi": 0, "dir": 0, "csinfo": 0, "interp": 0, "csdata": 0}
        section = ENDFSection(head,file)
        self.sections[section.getMT()] = section
        if self.file == 1:
            self.section_data = section.getSectionData()
        try:
            while(True):
                head = ENDFRecord(file.readline())
                section = ENDFSection(head,file)
                self.sections[section.getMT()] = section
        except ENDException as e:
            #print("Found END Record: %s" % (e.getType()))
            print("Finished parsing file MF: %d" % self.file)

        #Validation

    def persist(self):
        for section in self.sections.values():
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
    def __init__(self, file):
        head = ENDFRecord(file.readline())
        if head.isTEND():
            raise ENDException(ENDFRecordType.TEND)
        self.material = head.getMAT();
        self.mat_key = None
        self.lib_key = None
        self.file_key = None
        file1 = ENDFFile(head,file)
        section_data = file1.getSectionData()
        self.timings = {"total": 0, "lib": 0, "mat": 0, "gi": 0, "dir": 0, "csinfo": 0, "interp": 0, "csdata": 0}
        #self.files = [file1]
        self.files = {}
        self.files[file1.getFile()] = file1
        try:
            while(True):
                head = ENDFRecord(file.readline())
                #print("in mat file head: MAT: %s MF: %s MT: %s" % (head.getMAT(),head.getMF(), head.getMT()))
                endffile = ENDFFile(head,file)
                self.files[endffile.getFile()] = endffile
        except ENDException as e:
            #print("Found END Record: %s" % (e.getType()))
            print("Finished parsing Material MAT: %d" % self.material)
            for timing in self.timings:
                if self.timings.get(timing) > .001:
                    print(f"Persisted {timing} in {self.timings.get(timing):0.4f} seconds")           
                
    def persist(self):
        for file in self.files.values():
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
            #with open(self.filename, "r") as file:
            if self.zip:
                file = io.TextIOWrapper(self.archive.open(self.filename, "r"),encoding ='ISO-8859-1')
            else:
                file = open(self.filename, "r", encoding ='ISO-8859-1')
            # First record is TPID
            rec = ENDFRecord(file.readline())
            rec.setTPID()
            self.TPID = rec
            self.NTAPE = rec.getMAT()
                
            # Parse Materials until TEND
            while(True):
                self.materials.append(ENDFMaterial(file))


        except IOError:
            print('Error While Opening File: %s' % (self.filename))  
        except ENDException as e:
            #print("Found END Record: %s" % (e.getType()))
            print("Finished parsing tape: %s" % (self.filename))
        finally:
            if file is not None:
                file.close()

    def parseTape_new(self):
        file = None
        try:
            self.materials = []
            if self.zip:
                file = io.TextIOWrapper(self.archive.open(self.filename, "r"),encoding ='ISO-8859-1')
            else:
                file = open(self.filename, "r", encoding ='ISO-8859-1')
            data = numpy.genfromtxt(file, dtype="U66,i2,i1,i2,i4", names=['content','MAT','MF','MT','NS'],
                delimiter=[66,4,2,3,5])

            #print(type(data['MAT']))
            #print(data['MAT'])
            print(numpy.unique(data['MAT'], return_counts=True))
            print(numpy.argwhere((data['MF']==0) & (data['MAT']!=0)))
            idx13 = numpy.argwhere(data['MAT']==13)
            print(data[idx13])
            # First record is TPID
            rec = ENDFRecord(data[0])
            rec.setTPID()
            self.TPID = rec
            self.NTAPE = rec.getMAT()
                
            # Parse Materials until TEND
            nrec = len(data)
            #while(True):
            for i in range(1,nrec):
                self.materials.append(ENDFMaterial(data[i]))


        except IOError:
            print('Error While Opening File: %s' % (self.filename))  
        except ENDException as e:
            #print("Found END Record: %s" % (e.getType()))
            print("Finished parsing tape: %s" % (self.filename))
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
#tape.parseTape_new()

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
