import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from db_conn import ClickhouseDB, OracleDB
from conf import log_folder
from utils import error_handler, timeit
import os.path
import uuid
import zipfile

conn = ClickhouseDB()
conn_odb = OracleDB()

msp_registry = []
#okved_list = []
lic_list = []
output_products_list = []
partnership_list = []
contracts_list = []
agreements_list = []

def get_agreements(document_node, uuid, inn, ogrn, mspdate):
    for contract in document_node.findall('.//СвКонтр '):
        agr_dict = {}
        agr_dict['UUID'] = uuid
        agr_dict['INN'] = inn
        agr_dict['OGRN'] = ogrn
        agr_dict['ClientTitle'] = contract.get('НаимЮЛ_ЗД')
        agr_dict['ClientINN'] = contract.get('ИННЮЛ_ЗД')
        agr_dict['AgrDesc'] = contract.get('ПредмДог')
        agr_dict['AgrID'] = contract.get('НомДогРеестр')
        agr_dict['AgrDate'] = datetime.strptime(contract.get('ДатаДог'), '%d.%m.%Y').date() if contract.get(
            'ДатаДог') else None
        agr_dict['MSPDate'] = mspdate
        agreements_list.append(agr_dict)

def get_contracts(document_node, uuid, inn, ogrn, mspdate):
    for contract in document_node.findall('.//СвКонтр'):
        cont_dict = {}
        cont_dict['UUID'] = uuid
        cont_dict['INN'] = inn
        cont_dict['OGRN'] = ogrn
        cont_dict['ClientTitle'] = contract.get('НаимЮЛ_ЗК')
        cont_dict['ClientINN'] = contract.get('ИННЮЛ_ЗК')
        cont_dict['ContractDesc'] = contract.get('ПредмКонтр')
        cont_dict['ContaractID'] = contract.get('НомКонтрРеестр')
        cont_dict['ContractDate'] = datetime.strptime(contract.get('ДатаКонтр'), '%d.%m.%Y').date() if contract.get('ДатаКонтр') else None
        cont_dict['MSPDate'] = mspdate
        contracts_list.append(cont_dict)

def get_partnerships(document_node, uuid, inn, ogrn, mspdate):
    for prtrnshp in document_node.findall('.//СвПрогПарт'):
        prntr_dict = {}
        prntr_dict['UUID'] = uuid
        prntr_dict['INN'] = inn
        prntr_dict['OGRN'] = ogrn
        prntr_dict['ClientTitle'] = prtrnshp.get('НаимЮЛ_ПП')
        prntr_dict['ClientINN'] = prtrnshp.get('ИННЮЛ_ПП')
        prntr_dict['ContaractID'] = prtrnshp.get('НомДог')
        prntr_dict['ContractDate'] = datetime.strptime(prtrnshp.get('ДатаДог'), '%d.%m.%Y').date() if prtrnshp.get('ДатаДог') else None
        prntr_dict['MSPDate'] = mspdate
        partnership_list.append(prntr_dict)

def get_output_products(document_node, uuid, inn, ogrn, mspdate):
    for product in document_node.findall('.//СвПрод'):
        prod_dict = {}
        prod_dict['UUID'] = uuid
        prod_dict['INN'] = inn
        prod_dict['OGRN'] = ogrn
        prod_dict['OutputProductCode'] = product.get('КодПрод')
        prod_dict['OutputProductDesc'] = product.get('НаимПрод')
        prod_dict['OutputProductInnovative'] = int(product.get('ПрОтнПрод')) if product.get('ПрОтнПрод') else None
        prod_dict['MSPDate'] = mspdate
        output_products_list.append(prod_dict)

def get_licences(document_node, uuid, inn, ogrn, mspdate):
    for licence in document_node.findall('.//СвЛиценз'):
        lic_dict = {}
        lic_dict['UUID'] = uuid
        lic_dict['INN'] = inn
        lic_dict['OGRN'] = ogrn
        lic_dict['LicSeries'] = licence.get('СерЛиценз')
        lic_dict['LicNmb'] = licence.get('НомЛиценз')
        lic_dict['LicType'] = licence.get('ВидЛиценз')
        lic_dict['LicDate'] = datetime.strptime(licence.get('ДатаЛиценз'), '%d.%m.%Y').date() if licence.get('ДатаЛиценз') else None
        lic_dict['LicStartDate'] = datetime.strptime(licence.get('ДатаНачЛиценз'), '%d.%m.%Y').date() if licence.get('ДатаНачЛиценз') else None
        lic_dict['LicEndDate'] = datetime.strptime(licence.get('ДатаКонЛиценз'), '%d.%m.%Y').date() if licence.get('ДатаКонЛиценз') else None
        lic_dict['LicIssuedBy'] = licence.get('ОргВыдЛиценз')
        lic_dict['LicSuspendedDate'] = datetime.strptime(licence.get('ДатаОстЛиценз'), '%d.%m.%Y').date() if licence.get('ДатаОстЛиценз') else None
        lic_dict['LicSuspendedBy'] = licence.get('ОргОстЛиценз')
        lic_dict['LicActivity'] = licence.get('НаимЛицВД')
        lic_dict['MSPDate'] = mspdate
        lic_list.append(lic_dict)

# def get_okveds(document_node, uuid, inn, ogrn, mspdate):
#     for okveds in document_node.findall('.//СвОКВЭД'):
#         for okved in okveds.findall('.//СвОКВЭДОсн'):
#             okved_dict = {}
#             okved_dict['UUID'] = uuid
#             okved_dict['INN'] = inn
#             okved_dict['OGRN'] = ogrn
#             okved_dict['OKVEDType'] = 1
#             okved_dict['OKVED'] = okved.get('КодОКВЭД')
#             okved_dict['OKVEDDesc'] = okved.get('НаимОКВЭД')
#             okved_dict['OKVEDVersion'] = okved.get('ВерсОКВЭД')
#             okved_dict['MSPDate'] = mspdate
#             okved_list.append(okved_dict)
#         for okved in okveds.findall('.//СвОКВЭДДоп'):
#             okved_dict = {}
#             okved_dict['UUID'] = uuid
#             okved_dict['INN'] = inn
#             okved_dict['OGRN'] = ogrn
#             okved_dict['OKVEDType'] = 2
#             okved_dict['OKVED'] = okved.get('КодОКВЭД')
#             okved_dict['OKVEDDesc'] = okved.get('НаимОКВЭД')
#             okved_dict['OKVEDVersion'] = okved.get('ВерсОКВЭД')
#             okved_dict['MSPDate'] = mspdate
#             okved_list.append(okved_dict)

def transform_and_save():
    error_handler('I', 'Создание дейта фреймов')
    df_msp_registry = pd.DataFrame(msp_registry, dtype=object)
    df_licenses = pd.DataFrame(lic_list, dtype=object)
    df_outcomeproduct = pd.DataFrame(output_products_list, dtype=object)
    df_partnership = pd.DataFrame(partnership_list, dtype=object)
    df_contracts = pd.DataFrame(contracts_list, dtype=object)
    df_agreements = pd.DataFrame(agreements_list, dtype=object)

    conn.insert_df('msp_registry', df_msp_registry)
    conn.insert_df('msp_licences', df_licenses)
    conn.insert_df('msp_OutputProducts', df_outcomeproduct)
    conn.insert_df('msp_partnership', df_partnership)
    conn.insert_df('msp_contracts', df_contracts)
    conn.insert_df('msp_agreements', df_agreements)

@timeit
def parse_xml(msp_zip):
    #zf = zipfile.ZipFile(r'E:\temp\data-08262016-structure-08012016.zip', 'r')
    conn.check_duplicate_zip(msp_zip)
    zip_path = rf'{log_folder}\{msp_zip}'
    if os.path.isfile(zip_path) is False:
        error_handler('E', f'Архив {zip_path} не существует')
        raise

    mspdate = None
    zf = zipfile.ZipFile(zip_path, 'r')

    zip_count = 1
    zip_all = len(zf.infolist())
    error_handler('I', f'Кол-во XML-файлов в архиве: {zip_all}')
    for name in zf.namelist():
        f = zf.open(name)
        error_handler('I', f'Обработка {zip_count}/{zip_all}, файл {f.name}')
        zip_count += 1
        parser = ET.XMLParser(encoding="utf-8")
        tree = ET.parse(f, parser=parser)
        root = tree.getroot()

        for doc_elem in root.findall('.//Документ'):
            object_dict = {}
            object_dict['UUID'] = uuid.uuid4()
            object_dict['Type'] = int(doc_elem.get('ВидСубМСП'))

            if object_dict['Type'] == 1:
                for company in doc_elem.findall('.//ОргВклМСП'):
                    object_dict['INN'] = company.get('ИННЮЛ')
                    object_dict['OGRN'] = company.get('ОГРН')
                    object_dict['Title'] = company.get('НаимОрг')
                    object_dict['ShortTitle'] = company.get('НаимОргСокр')
            elif object_dict['Type'] in [2, 3]:
                for enterpr in doc_elem.findall('.//ИПВклМСП'):
                    object_dict['INN'] = enterpr.get('ИННФЛ')
                    object_dict['OGRN'] = enterpr.get('ОГРНИП')
                    for person in enterpr.findall('.//ФИОИП'):
                        object_dict['Title'] = ' '.join(filter(None, (person.get('Фамилия'), person.get('Имя'), person.get('Отчество'))))

            if mspdate is None:
                mspdate = datetime.strptime(doc_elem.get('ДатаСост'), '%d.%m.%Y').date()
            object_dict['MSPDate'] = datetime.strptime(doc_elem.get('ДатаСост'), '%d.%m.%Y').date()
            object_dict['MSPIncludedDate'] = datetime.strptime(doc_elem.get('ДатаВклМСП'), '%d.%m.%Y').date()
            object_dict['MSPType'] = int(doc_elem.get('КатСубМСП'))

            for reg in doc_elem.findall('.//СведМН'):
                object_dict['Region'] = int(reg.get('КодРегион'))

            object_dict['PublicInterest'] = int(doc_elem.get('СведСоцПред')) if doc_elem.get('СведСоцПред') else None
            object_dict['NumberOfEmployees'] = int(doc_elem.get('ССЧР')) if doc_elem.get('ССЧР') else None
            object_dict['File'] = f.name
            msp_registry.append(object_dict)

            get_licences(doc_elem, object_dict['UUID'], object_dict['INN'], object_dict['OGRN'], object_dict['MSPDate'])
            #get_okveds(doc_elem, object_dict['UUID'], object_dict['INN'], object_dict['OGRN'], object_dict['MSPDate'])
            get_output_products(doc_elem, object_dict['UUID'], object_dict['INN'], object_dict['OGRN'], object_dict['MSPDate'])
            get_partnerships(doc_elem, object_dict['UUID'], object_dict['INN'], object_dict['OGRN'], object_dict['MSPDate'])
            get_contracts(doc_elem, object_dict['UUID'], object_dict['INN'], object_dict['OGRN'], object_dict['MSPDate'])
            get_agreements(doc_elem, object_dict['UUID'], object_dict['INN'], object_dict['OGRN'], object_dict['MSPDate'])

    transform_and_save()
    conn.save_zip(msp_zip, mspdate)

def get_clients():
    sql = '''select distinct icusnum, ccusnumnal from xxi.cus c 
                            inner join acc a on c.icusnum = a.IACCCUS
                            where ccusnumnal is not null and length(ccusnumnal) in (10, 12)
                                and c.ccusflag <> '1'
                                and c.ICUSSTATUS = 2 
                                and ccusnumnal <> '6829000028'
                                
                                '''
    try:
        with conn_odb.connection.cursor() as cursor:
            cursor.execute(sql)
            error_handler('I', 'Получение клиентов из Oracle')
            rows = cursor.fetchall()
            results = []
            if rows:
                for row in rows:
                    client = {'ICUSNUM': row[0], 'CCUSNUMNAL': row[1]}
                    results.append(client)
            return results

    except Exception as error:
        error_handler('E', f'Ошибка получения клиентов {str(error)}')
        raise

@timeit
def get_msp_status(clients):
    list = []
    for client in clients:
        results = conn.get_msp_status(client['ICUSNUM'], client['CCUSNUMNAL'])
        list.extend(results)
    return list

if __name__ == '__main__':
    msp_zip = ''
    while len(msp_zip.strip()) == 0:
        msp_zip = input(f'Введите название zip-архива из папки "{log_folder}" без кавычек\n')
        if len(msp_zip.strip()) == 0:
            error_handler('E', 'Имя файла не может быть пустым')
    result = msp_zip.endswith('.zip')
    if result is False:
        msp_zip=msp_zip+'.zip'
    parse_xml(msp_zip)

    clients = get_clients()
    clients_msp = get_msp_status(clients)
    conn_odb.execute_many(clients_msp)

    conn.close()
    conn_odb.close()

