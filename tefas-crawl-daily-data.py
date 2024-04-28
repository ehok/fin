import concurrent.futures
import mysql.connector
import requests
from bs4 import BeautifulSoup
import logging
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

log_level = config.get('logging', 'level')
numeric_level = getattr(logging, log_level.upper(), None)
if not isinstance(numeric_level, int):
    raise ValueError(f'Invalid log level: {log_level}')

logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(message)s') # filename=log_file,

db_config = {
    'user': config.get('database', 'user'),
    'password': config.get('database', 'password'),
    'host': config.get('database', 'host'),
    'database': config.get('database', 'database')
}

fund_codes = ['AVJ','AVY','AVE','AEL','GEV','AHJ','AVH','AE4','MZN','AE1','AE2','AEK','AVP','AVG','AE3','AEH','AEB','AVU','MZL','AVL','AVB','AVD','FYL','FYN','AEI','AEG','VVA','VVZ','VVE','VVD','VVU','AYJ','VVM','AAJ','AVK','AVO','AVN','GFH','AVR','MZP','CJF','SSO','PUD','FUM','ANL','AOY','AFO','LTC','PAL','AFA','AFV','AJL','BNC','BND','ADP','BLH','APJ','BDY','AKU','BOE','ABU','ARL','BHE','CVE','PUC','PUR','URG','BVC','BUY','VAY','ADE','CJH','DTZ','AJN','DSH','VCY','AVZ','AKE','ZFB','UCN','URV','GUM','HVV','HYY','HZV','AK3','PAI','FUA','AJK','AGC','ARM','CVB','ALC','KHO','AVT','GGC','AIS','KNP','ZLG','MTV','AIM','PAP','KNZ','LHM','OBN','OBR','ODN','ONK','URC','OUN','KNV','URS','APT','URM','OPZ','PUT','ARP','CVD','ARF','CVC','AZZ','UHV','CVA','KNC','UHN','BVB','AYR','ALE','AES','PUV','AZF','APP','AIH','SAS','AFS','CJG','CJD','CVF','TAR','ICZ','TGR','AK2','ODV','BGP','AJE','KNT','LHP','YDH','AFT','YRB','SER','ONN','MPD','MJL','MKG','MMH','MJB','ONE','CIN','ESG','MJH','MJG','ASJ','MPN','EYT','BUL','MPS','MPF','NKL','MPK','IDL','GPT','MLS','MTS','CPU','TFE','DOL','RTI','AL4','RBA','KAC','KSM','KSL','AL7','VKR','RBK','RBH','AL5','MJE','PVK','RBV','RBT','RPU','VKI','AL6','CAF','NDL','RMA','RYA','SLN','AS3','TMH','LLA','ABJ','BBI','HVU','BVM','HVZ','HVT','AS1','DEF','ESN','GIH','BGI','AEV','LID','IHZ','IHA','AOJ','NDI','KHT','KST','PRU','VOF','AZD','AZK','AZA','AUG','AZH','AZM','AZL','AZS','AZY','ALU','AMZ','AEZ','ALZ','ACV','AEU','AMG','ALS','AUA','AEN','AMR','ALR','AMY','FYY','FYU','AMF','KOE','ALI','ALJ','AIP','AIE','ANJ','ANP','KOS','KOA','AFJ','APG','AEC','AEP','YZD','AHL','BGL','AEA','AH0','AO1','AO2','AH6','AH3','HS1','AG2','AH9','AH5','AG3','AH1','AG1','AHB','AH4','ATE','AER','AGE','AET','AJA','AJG','AJB','AJH','AJC','AFP','AFH','AJF','AHC','AH2','ATK','BHS','TVG','BHT','BNO','AH8','AG4','MDD','ABE','LRT','PKF','THV','TLZ','YCL','AED','AAK','AUT','ANZ','AAS','NKJ','JET','AAV','AVC','AYA','AAL','RTG','RSF','YLC','DGH','NKK','LGO','TAL','AP2','AHI','BAC','AHU','ABG','PFS','SSE','DFI','PPT','ZVB','AHN','GCD','GLM','BDC','IYB','ESP','TLH','TPV','TLE','HEA','HEL','HEG','HEB','RZN','HES','HED','HEE','HER','RZM','HET','AJR','AJZ','AJP','AJY','AJV','HEI','HEC','AJT','HEP','HEK','SPD','FRA','SPN','TKH','SPE','SPA','GLS','PAC','PUA','THS','GP1','PPF','LCT','RKL','UHL','FYZ','SRA','VFS','NTB','BAO','PBB','SPG','BGH','BLG','TNP','HJA','AZV','PBS','BUT','BUP','HUI','TRN','AII','ATJ','GMA','BJO','DZP','AHH','DFO','FDV','VCG','UFH','GBZ','EHS','IJF','AIR','FNE','FRZ','GSL','BVT','GSG','GFT','HPJ','NOV','PRF','AIG','KSK','VCD','LZV','EJG','IZA','HYP','IZM','KFK','KTI','KVS','KMF','PTG','PUZ','MRT','MSK','YCS','NHT','DOR','ORI','FYG','PPZ','PNR','FYV','NTC','PSS','GNK','SHS','SRE','SRY','SRO','SVS','SYL','TAZ','TJL','TEJ','TSP','TTN','BVY','USS','FYM','VTF','PYF','PYH','PYL','HMT','BJD','DOD','HGT','FDZ','DVU','IBJ','FYJ','THO','THP','IOT','ZMT','FYI','GSE','PSL','PSG','AHZ','GTF','GL1','IBG','AHV','FYB','GSP','GBL','IBR','FYH','AZJ','GBC','PAF','AP6','BST','BHH','BSE','BDI','BBF','BHF','AC7','BIH','OME','DHI','DRH','AC6','ICH','AC8','KHA','IAU','AC2','AC5','KHC','AC1','MGE','OHI','AC4','SHI','CSH','AC3','AP7','AGA','TKV','AGB','AGG','AGH','AGT','AGD','AGM','BEO','BEF','BEI','BNA','PRS','PRC','BPU','BPL','BPF','BPE','BPH','BPK','BPC','BNK','BNB','BPI','BPS','BPJ','BPN','BPO','BNZ','BPR','BNL','BPG','BNS','BZY','BVH','BIS','BVI','BHI','BVZ','KYR','BTE','BVV','DBA','DDA','DNA','DAT','DAI','YBJ','DDE','DCE','DZM','DBG','DTL','DZE','BLE','DBP','DSP','DVO','DPT','DBB','DAC','DHS','THH','DTV','DCP','CGD','DP1','DP7','DDF','DOA','DDC','DP3','DVZ','DPE','DYN','DFD','DLG','DHM','DBH','DSG','DPC','DP4','DRS','DMG','DVN','DAH','DPP','HOM','DXP','DSD','DPI','DP8','DP6','DKA','DUT','DKV','KRO','DK8','DCV','DYJ','DP9','DKH','DBK','DCB','DPK','DKS','PLS','DVC','DP5','DVT','MFT','MSL','DNO','DNL','DPN','DPL','DVI','DOS','DVA','DAS','DOI','DOV','ORS','DBZ','DP2','DLY','DPG','DSC','DTR','DPB','DDP','DCD','DMV','DAL','DLD','DFC','DHJ','TMP','DAZ','DPZ','DKD','DZG','DMZ','SSN','FIB','FZJ','FSG','FBI','FVL','FJB','FIT','FID','FPE','FJZ','FSH','FPH','FSR','FBZ','FPK','FMR','FJN','FJM','FJC','FHP','FSF','FIL','FBN','FZP','SKT','SLF','FTM','FFD','GO1','GO9','FS5','FS1','FCK','GO4','GO2','FS2','FBC','FS3','GO6','GO3','FS4','FDG','FRC','FPI','FDN','GVL','GHA','GCV','GDV','GCT','GEK','GHF','GEU','GHG','GHK','GHE','GEF','GEH','GEG','GCY','GEA','GHL','GES','GHI','GKB','GCN','GHT','GHJ','GHU','GHN','GHV','GHM','GHY','GCK','GCS','GHP','GHZ','GEL','GHD','GHH','GED','GHO','GKH','GVB','GVZ','TGA','GFK','GTA','GOL','GGP','GGN','GFD','GBJ','GJF','GBN','GOH','GUA','GPB','GZP','GBH','GPF','GTL','GPL','GAE','GBV','GA1','GFN','GDJ','GJE','GSR','GZU','HAG','EGP','GVA','GZE','GVC','GEZ','GZV','GPA','GZZ','GIE','GMP','GGM','GTZ','GFO','GYN','GHS','GCA','INH','GPI','GZJ','GAL','GPC','EUN','GTK','GVD','GCI','GKV','KDT','TGT','GMJ','GMG','MET','GFB','GFL','GPM','GAH','HNN','GFA','GMV','GFY','GJO','GPV','GKK','GGR','GPH','GZD','GJA','GJD','GRO','GZB','GUE','OPP','GSO','GZO','GJH','GPJ','GLP','GZG','EUZ','GEC','GZR','GFE','GZL','GYR','GYG','GTM','GZH','GTY','GZY','GNL','GPU','GVI','GPZ','GAS','GUH','GKL','GBP','GYC','GAC','GVY','GAV','GAU','GCC','GAJ','GCZ','GJM','ECA','GMD','EC2','GZN','GLG','GZM','EBD','GLC','GKF','ECV','GLV','ECB','FJG','FIM','GRA','EIE','FEA','FIE','FIY','EIG','BGK','FER','BEE','BGE','EIK','FVI','BEK','BEH','BBH','FFZ','FIF','FEO','BKB','FIK','FII','FIS','FEI','FIC','FIU','FIH','FIV','FIG','FGH','FIZ','FGF','FIR','EHG','EIH','EIF','EST','FET','FEN','FEF','FFC','FES','HLR','HYK','HEH','HAM','HDJ','HJJ','HYZ','HAT','HKM','HVK','HJB','NNF','HPL','HDH','HBV','HDA','HVA','HMK','HMC','HGH','HGJ','UGH','HPI','HID','HGM','HIS','HFI','HIM','HPP','HIN','HKP','HKH','HKJ','HMR','KVK','KSA','HPF','HP3','LTS','HMV','FNT','HVN','NFK','HYV','HVL','HVB','HVC','HGV','HDS','HTZ','DUH','HUS','HPU','HDK','HPZ','UGL','HST','HAE','HFA','HBF','HBN','HBU','HPO','HPD','HSA','HME','HGU','HVS','HSP','HKR','HPT','HGC','HSL','HMS','HNS','HOA','HOY','HII','HMG','HFV','ICA','ICD','IJH','IFV','ICV','ICF','ICC','ICE','ICS','IFN','GAN','GGK','IV2','IJA','IV7','IHE','GPG','GJB','GAF','GAG','GYK','CVK','IDN','IUH','NES','IEZ','IUT','GEI','IFG','IUC','GBG','IHC','GMR','GKS','INV','INZ','GAK','GYL','ICU','IRY','GSM','IV5','IRT','IV6','IUR','IAE','IZS','IIP','IAF','FYA','IZV','IVA','IBE','IPB','IRF','IZB','IJV','IVY','ACC','IAR','EES','IHY','ILJ','ACK','IJI','ACZ','ACD','IVS','IIS','IRV','IUA','IVF','IJK','IRL','IST','YCY','ACN','IMF','IBM','IOM','IIA','IIE','ION','ILM','IIC','IFY','IOV','IRO','ITJ','IDP','IJL','ACU','IIH','ICN','ARE','IYP','CTP','IDV','IRA','TTA','ILK','ITV','IMH','ILE','IPE','IUZ','ICG','ITA','IPU','CTF','IBB','IPG','ITZ','LFD','IBD','IUV','TAU','TTE','IDH','TIE','CKS','ISR','ILH','IJP','BTY','CSD','IBC','IPC','IZZ','ISS','ITL','TI7','KKH','IJB','IHN','KPK','IJE','GME','IPJ','TGE','IPV','IUB','EPO','CTG','IFD','IBP','IOG','IGF','IYR','TI2','IUM','IPR','IHT','IOO','ILI','TI3','IHK','JOT','KLM','IRE','CTM','TIV','IAT','IPK','KOZ','TKK','TSI','IBK','IMS','CTV','MKL','FBV','GMZ','IAC','IMO','IOP','IZL','IUS','ONL','IDB','INR','IDO','IOS','ONS','IOH','IOJ','IPO','ILC','IZY','TI6','IOL','TBV','TI1','ILP','IMZ','TBP','TPE','TPR','TPO','RAN','YAR','IKL','ILR','ICK','IDF','SSD','IJZ','IMY','SMP','BIO','TMZ','IPA','IYS','IJT','IEV','ITP','TMC','TI4','ILZ','EDU','ITR','ITD','IMT','IAI','IJS','IUU','IZE','UST','IGM','IGZ','TDG','TMG','IJC','IUF','IUN','IKP','SIZ','KRC','KRF','KUB','KYA','KDI','KPB','KPP','KRS','KRT','KES','KEF','KEY','KEG','KED','KEK','KEH','KEA','KEB','KKV','KJM','KKS','KEZ','KET','KTZ','KSU','KLT','KZL','KAV','KDL','KDK','KHU','KTR','KTM','KTE','KCV','KTT','KKB','KNJ','KPU','KTS','KKT','KMT','KPC','KFZ','KSV','KTV','KUT','KTN','KMA','KSG','KJK','KMS','KTU','KNS','KLU','KDZ','ZAY','KSR','KTJ','KLL','KKE','KDS','IDD','MSH','MMA','MSB','MVK','MAC','MAS','MSR','MAD','MBL','MPI','MJK','MPP','MEA','MHB','MHC','MHG','MHE','MHY','MHH','MHD','MHA','MHS','MHT','MHR','MHV','MHO','MHU','MHN','MHI','MHZ','MHM','MHL','MHK','MEY','MEV','NBH','NAU','OSF','NRC','NHP','NHY','NRG','NBZ','NSK','NZH','NVB','NRM','NMG','NVZ','NSA','NZT','NTF','NVT','NVC','NP1','NHA','IEG','IEA','IEE','IEF','IEH','IEK','IGE','IER','NHM','IEB','NHN','NJF','NUG','NUB','NJR','NJY','NJG','NHV','NSD','NKS','NYH','NVP','NUV','PPN','NSS','NTS','NTO','NCS','OPF','OIL','OPD','OUD','OPB','OLE','OPH','ODP','OHY','OIR','OPL','OPI','OLA','OSL','OSH','OSD','OUR','OUB','OGD','OFB','OKT','OKD','OFS','OHB','OTK','OKP','OYL','OYT','ODD','OBI','OBP','OFI','ODS','OKF','OHK','OTJ','OGF','OFK','OFA','OTF','OYS','IPP','PJP','PPD','PHS','PTC','IPF','PPP','PCN','PPB','PPS','PBF','PFO','PJL','PPE','PBR','PHE','PRY','BCC','OJH','KSD','PBK','PDF','OUY','ORC','OZC','EIB','EID','EIC','KCN','IZF','KOP','OIS','PDD','ELZ','KIS','EKF','OMB','MSO','IAS','EIL','OPU','MAV','GRL','OJK','FGA','OJA','FUS','OAB','FUP','FP1','FFF','OJB','BID','FNO','FYD','FPZ','FI3','CBN','FFH','OVT','OCT','OVF','OVD','FUB','OVR','FFP','FGS','FMG','OGV','FHZ','OFO','KRV','FDE','DKP','FDY','FKH','FSV','FTY','FKM','FKV','FYT','FKE','LET','OAC','OTS','FSE','OAV','OJN','OFH','OMC','FYO','FSK','FI5','DJA','FMS','OLC','OCN','OZF','FFO','OCM','FDO','OTZ','OTE','OJT','OLD','OJU','FVS','OJY','OJF','CHM','CHT','CHO','CHL','CHS','CFA','CFB','CFY','CHH','CHK','CGG','CHN','CGE','CFD','CFK','CHG','CHD','CHA','CHI','CFE','CHC','CHU','CFC','RBR','RJG','RBN','RPD','RTH','RBI','RTP','RDH','RIK','RIH','RKV','RUH','RPG','RAF','RBE','RPS','RCL','CEY','RCS','CPT','RDF','RDS','RPE','RDK','RPO','RHD','RHS','RPT','RPC','RPM','RPN','ROD','RVI','RVS','ROB','ROY','RTA','ROF','RTB','RPP','PIL','PRZ','RAY','RZR','RSZ','RPK','RPX','RYF','SFA','SVB','SBS','AN1','ST1','STS','SBR','SBI','FPR','STZ','SJP','KBJ','FYF','TJB','TPF','TCD','TKF','TCI','TTL','TGV','TNS','NDC','THG','TCB','TNH','TZP','TCC','TCS','TGZ','TTV','TAO','ONB','TUA','TJT','TFF','TRR','YZH','TE4','TPC','TPJ','TBT','BRG','TCG','TB9','TTS','TTZ','TPL','GMC','TYH','TNB','TNI','TIP','IGL','IDY','TJI','TDP','TVR','TCN','TNK','TPZ','TMR','MTX','TE3','MGH','TOK','ONT','TLU','TNF','SHE','TOT','TKM','TPP','TBE','TBZ','TJF','TGX','TP1','TCF','TTP','TLT','TIT','TMV','TLY','TMM','T3B','TVN','THD','KID','KIF','KIA','KIE','KIB','NIG','HHE','VGA','VKJ','VKE','HHB','VEG','TML','VES','ZHE','VEE','VGE','VEH','TYJ','ZHG','HHG','VET','ZHD','HHY','HHM','VEB','HHN','VEY','VYB','KRM','VER','VGB','ZHF','VEI','VGD','TMN','VGF','VEO','VGH','VEV','VGG','VGC','VGK','VED','VGZ','VEP','VEL','VGT','VGY','VEK','ZHB','TBJ','VEU','VGP','TJY','TBH','UP1','UAP','UPS','SUA','UAB','USY','UYH','UPD','UPH','SUB','UZY','UJA','ULH','UP2','UPP','ULL','NTD','SUC','GGL','VNK','VMV','ANS','ANE','AEY','ANK','ANG','YDL','YLR','YPD','YPF','YIV','YUY','YKT','YPL','YCP','YZM','YVS','YLE','YHZ','YHB','YEF','YDP','YAN','YHS','YUD','YOT','YNL','YDK','YTJ','YLO','YGM','YVF','YBE','YP4','YZC','YPK','YZG','YAC','YVD','YPC','YKS','YUN','YHP','YZK','YSL','YUB','YAK','YCG','YHK','YHT','YLY','YFV','YAS','YTR','YMD','MOD','YDI','YVO','YNK','YQA','YCH','YDZ','YRF','YRO','YJO','YBS','YLB','YPT','YP2','YJA','YBP','YOA','YCK','YZL','YZF','YP1','PYS','KSY','YUI','YPP','YPZ','YJU','YJK','YVG','YTY','YTV','YJH','YP3','YSU','YPV','YTD','YAY','YJY','ZDZ','ZP9','ZGD','TCA','ZCD','ZFZ','ZP1','ZP5','ZP4','ZBJ','ZYD','ZPP','ZBP','ZSR','ZJL','ZLH','ZJV','ZBB','ZEA','ZEO','ZTF','ZSA','ZJB','TZT','ZPN','TZC','DMR','ZBD','ZCG','ZYC','ZJT','ZCC','ZCN','ZSG','ZPC','ZBZ','HKV','HPV','HLL','HPC','TZD','ZSB','ZP2','ZBO','VFK','ZJI','ZCV','ZFH','ZPF','ZPE','ZKK','ZPJ','ZKP','ZKE','ZP8','TZV','ZPK','ZPG','ZMY','ZCB','ZCH','ZBN','ZZL','ZR2','ZR3','ZSN','ZCA','ZDK','TZL','ZRE','ZP6','ZCF','ZPA','ZNF','ZSF','ZSK','ZDD','ZP7','ZUD','ZUE','ZJR','TZH','ZP3','ZVO','VKT','VK6','ZJH','ZCE','ZTM','ZPT']
base_url = "https://www.tefas.gov.tr/FonAnaliz.aspx?FonKod={}"

def connect_to_db(config):
    try:
        conn = mysql.connector.connect(**config)
        logging.info("Successfully connected to the database.")
        return conn
    except mysql.connector.Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None
    
def create_fund_table(cursor, fund_code):
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS `{fund_code}` (
            id INT AUTO_INCREMENT PRIMARY KEY,
            data_date DATE NOT NULL,
            son_fiyat DECIMAL(13, 6) NULL,
            gunluk_getiri DECIMAL(5, 4) NULL,
            pay_adet BIGINT NULL,
            fon_toplam_deger DECIMAL(20, 2) NULL,
            kategorisi VARCHAR(100) NULL,
            kategori_derecesi VARCHAR(10) NULL,
            yatirimci_sayisi INT NULL,
            pazar_payi DECIMAL(5, 2) NULL,
            ortalama_yatirim_miktari DECIMAL(20, 2) NULL,
            UNIQUE KEY unique_date (data_date)
        );
    """
    try:
        cursor.execute(create_table_query)
        logging.info(f"Table `{fund_code}` checked/created successfully.")
    except mysql.connector.Error as e:
        logging.error(f"Error creating table `{fund_code}`: {e}")

def extract_data(description, soup):
    for li in soup.find_all("li"):
        if description in li.text:
            span = li.find_next("span")
            if span and span.text:
                return span.text.strip().replace('%', '').replace('.', '').replace(',', '.')
    return None

def insert_or_update_data(conn, cursor, fund_code, data):
    select_query = f"""
        SELECT * FROM `{fund_code}` WHERE data_date = %s;
    """
    try:
        cursor.execute(select_query, (data['data_date'],))
        existing_row = cursor.fetchone()
        if existing_row:
            columns = cursor.column_names
            existing_data = dict(zip(columns, existing_row))
            data_changed = False
            for key in data:
                if str(data[key]) != str(existing_data[key.lower()]):
                    data_changed = True
                    break
            
            if data_changed:
                logging.warning(f"Existing data for {fund_code} on {data['data_date']} will be updated. Old data: {existing_data}")
                update_data(conn, cursor, fund_code, data)
            else:
                logging.info(f"No changes detected for {fund_code} on {data['data_date']}. No update necessary.")
        else:
            insert_data(conn, cursor, fund_code, data)
    except mysql.connector.Error as e:
        logging.error(f"Error checking/updating data for {fund_code}: {e}")
        conn.rollback()

def insert_data(conn, cursor, fund_code, data):
    insert_query = f"""
        INSERT INTO `{fund_code}` (data_date, son_fiyat, gunluk_getiri, pay_adet, 
                                   fon_toplam_deger, kategorisi, kategori_derecesi, 
                                   yatirimci_sayisi, pazar_payi, ortalama_yatirim_miktari)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        cursor.execute(insert_query, (
            data['data_date'], data['son_fiyat'], data['gunluk_getiri'],
            data['pay_adet'], data['fon_toplam_deger'], data['kategorisi'],
            data['kategori_derecesi'], data['yatirimci_sayisi'], data['pazar_payi'],
            data['ortalama_yatirim_miktari']
        ))
        conn.commit()
        logging.info(f"New data for {fund_code} on {data['data_date']} inserted successfully.")
    except mysql.connector.Error as e:
        conn.rollback()
        logging.error(f"Error inserting new data for {fund_code}: {e}")

def update_data(conn, cursor, fund_code, data):
    update_query = f"""
        UPDATE `{fund_code}`
        SET son_fiyat = %s, gunluk_getiri = %s, pay_adet = %s, fon_toplam_deger = %s,
            kategorisi = %s, kategori_derecesi = %s, yatirimci_sayisi = %s, pazar_payi = %s,
            ortalama_yatirim_miktari = %s
        WHERE data_date = %s;
    """
    try:
        cursor.execute(update_query, (
            data['son_fiyat'], data['gunluk_getiri'], data['pay_adet'],
            data['fon_toplam_deger'], data['kategorisi'], data['kategori_derecesi'],
            data['yatirimci_sayisi'], data['pazar_payi'], data['ortalama_yatirim_miktari'], data['data_date']
        ))
        conn.commit()
        logging.info(f"Data for {fund_code} on {data['data_date']} updated successfully.")
    except mysql.connector.Error as e:
        conn.rollback()
        logging.error(f"Error updating data for {fund_code}: {e}")

def process_fund_code(code):
    logging.info(f"Starting to process fund code: {code}")
    url = base_url.format(code)
    try:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        data = {
            'data_date': datetime.now().date(),
            'son_fiyat': extract_data("Son Fiyat (TL)", soup),
            'gunluk_getiri': extract_data("Günlük Getiri (%)", soup),
            'pay_adet': extract_data("Pay (Adet)", soup),
            'fon_toplam_deger': extract_data("Fon Toplam Değer (TL)", soup),
            'kategorisi': extract_data("Kategorisi", soup),
            'kategori_derecesi': extract_data("Son Bir Yıllık Kategori Derecesi", soup),
            'yatirimci_sayisi': extract_data("Yatırımcı Sayısı (Kişi)", soup),
            'pazar_payi': extract_data("Pazar Payı", soup),
            'ortalama_yatirim_miktari': float(extract_data("Fon Toplam Değer (TL)", soup)) / int(extract_data("Yatırımcı Sayısı (Kişi)", soup)) if int(extract_data("Yatırımcı Sayısı (Kişi)", soup)) != 0 else 0
        }

        if None not in data.values():
            conn = connect_to_db(db_config)
            if conn is not None:
                cursor = conn.cursor()
                create_fund_table(cursor, code)
                insert_or_update_data(conn, cursor, code, data)
                cursor.close()
                conn.close()
                logging.info(f"Successfully processed and updated data for fund code: {code}")
            else:
                logging.error("Could not connect to MySQL database - data insertion aborted.")
        else:
            logging.warning(f"Incomplete data for fund {code}, skipping insert.")
    except Exception as e:
        logging.error(f"Exception occurred while processing fund code {code}: {str(e)}")

def main():
    logging.info("Starting...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(process_fund_code, fund_codes)

if __name__ == '__main__':
    main()
