#! /usr/bin/env python3

import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import hashlib

#=========================
class mapper():
    ''' standard mapper class '''

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    #----------------------------------------
    def map(self, raw_data, input_row_num = None):
        json_list = []
        json_data = {}

        #--clean values
        #for attribute in raw_data:
        #    raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--mandatory attributes
        json_data['DATA_SOURCE'] = 'SPIRE'

        #--the record_id should be unique, remove this mapping if there is not one
        json_data['RECORD_ID'] = raw_data['imo']

        #--record type is not mandatory, but should be PERSON or ORGANIZATION
        json_data['RECORD_TYPE'] = 'VESSEL'

        #--column mappings

        # columnName: imo
        # 100.0 populated, 100.0 unique
        #      9968815 (1)
        #      9964510 (1)
        #      9481893 (1)
        #      9981752 (1)
        #      9968918 (1)
        json_data['IMO_NUMBER'] = raw_data['imo']

        # columnName: mmsi
        # 49.36 populated, 99.07 unique
        #      357723000 (3)
        #      374352000 (3)
        #      357699000 (3)
        #      351862000 (3)
        #      372406000 (3)
        json_data['MMSI_NUMBER'] = raw_data['mmsi']

        # columnName: name
        # 100.0 populated, 89.72 unique
        #      GUANGZHOU WENCHONG (31)
        #      HUANGPU WENCHONG (30)
        #      NINGBO XINLE (26)
        #      SHANGHAI WAIGAOQIAO (23)
        #      CHENGXI (22)
        json_data['VESSEL_NAME_ORG'] = raw_data['name']

        # columnName: ex_name
        # 23.07 populated, 97.9 unique
        #      ACT (7)
        #      Rebuilt (6)
        #      SANYO MARU (6)
        #      BALSA (6)
        #      Broken up (5)
        json_data['PRIOR_NAME_ORG'] = raw_data['ex_name']

        # columnName: vessel_type
        # 100.0 populated, 0.02 unique
        #      Bulk Carrier (19991)
        #      Tanker (16261)
        #      Dry Cargo (10627)
        #      Offshore (10045)
        #      General Dry Cargo (8420)
        json_data['VESSEL_TYPE'] = raw_data['vessel_type']

        # columnName: vessel_subtype
        # 71.91 populated, 0.41 unique
        #      Dry Cargo (10365)
        #      Bulk Carrier (5598)
        #      Petroleum Product Tanker (5309)
        #      Chemical Tanker (3507)
        #      Oil Tanker (2944)
        json_data['VESSEL_SUBTYPE'] = raw_data['vessel_subtype']

        # columnName: dwt
        # 96.71 populated, 39.63 unique
        #      1200 (325)
        #      1000 (244)
        #      1500 (221)
        #      2000 (202)
        #      49999 (201)
        json_data['dwt'] = raw_data['dwt']

        # columnName: gross_tonnage
        # 98.23 populated, 27.13 unique
        #      499 (913)
        #      1599 (383)
        #      498 (341)
        #      999 (287)
        #      749 (274)
        json_data['gross_tonnage'] = raw_data['gross_tonnage']

        # columnName: displacement
        # 6.82 populated, 49.15 unique
        #      17472 (37)
        #      45974 (29)
        #      47849 (26)
        #      58000 (25)
        #      25281 (24)
        json_data['displacement'] = raw_data['displacement']

        # columnName: grain_cubic_capacity
        # 26.97 populated, 31.92 unique
        #      71634 (438)
        #      97000 (367)
        #      78500 (222)
        #      77674 (139)
        #      72360 (120)
        json_data['grain_cubic_capacity'] = raw_data['grain_cubic_capacity']

        # columnName: liquid_cubic_98_percent
        # 23.48 populated, 50.13 unique
        #      174000 (103)
        #      170520 (72)
        #      84000 (72)
        #      3500 (59)
        #      5000 (58)
        json_data['liquid_cubic_98_percent'] = raw_data['liquid_cubic_98_percent']

        # columnName: net_tonnage
        # 57.11 populated, 23.19 unique
        #      19231 (159)
        #      149 (142)
        #      503 (123)
        #      20209 (87)
        #      19142 (87)
        json_data['net_tonnage'] = raw_data['net_tonnage']

        # columnName: teu
        # 14.63 populated, 15.92 unique
        #      1800 (129)
        #      4250 (88)
        #      1118 (84)
        #      15000 (73)
        #      140 (69)
        json_data['teu'] = raw_data['teu']

        # columnName: tpcmi
        # 10.24 populated, 25.48 unique
        #      52 (144)
        #      52.3 (111)
        #      52.4 (74)
        #      66.6 (70)
        #      58.8 (66)
        json_data['tpcmi'] = raw_data['tpcmi']

        # columnName: engine_designation
        # 83.94 populated, 8.01 unique
        #      6S50MC-C (2255)
        #      6S60MC (969)
        #      6S60MC-C (887)
        #      6S50MC (795)
        #      6S42MC (788)
        json_data['engine_designation'] = raw_data['engine_designation']

        # columnName: main_engine_designer
        # 85.07 populated, 0.31 unique
        #      MAN B&W (17778)
        #      B&W (13107)
        #      Sulzer (6940)
        #      Wartsila (4862)
        #      Mitsubishi (4088)
        json_data['main_engine_designer'] = raw_data['main_engine_designer']

        # columnName: main_engines
        # 83.98 populated, 0.02 unique
        #      1 (61938)
        #      2 (13203)
        #      4 (1837)
        #      3 (569)
        #      6 (261)
        json_data['main_engines'] = raw_data['main_engines']

        # columnName: mco
        # 83.11 populated, 8.11 unique
        #      9480 (1397)
        #      5958 (655)
        #      13560 (613)
        #      18660 (606)
        #      9960 (514)
        json_data['mco'] = raw_data['mco']

        # columnName: mco_unit
        # 96.3 populated, 0.0 unique
        #      KW (79549)
        #      BHP (9799)
        #      SHP (57)
        #      IHP (4)
        json_data['mco_unit'] = raw_data['mco_unit']

        # columnName: mcorpm
        # 55.19 populated, 0.81 unique
        #      127 (4339)
        #      105 (3808)
        #      750 (3077)
        #      1000 (2189)
        #      91 (1874)
        json_data['mcorpm'] = raw_data['mcorpm']

        # columnName: propellers
        # 42.49 populated, 0.02 unique
        #      1 (33304)
        #      2 (5893)
        #      3 (162)
        #      4 (57)
        #      6 (23)
        json_data['propellers'] = raw_data['propellers']

        # columnName: propulsion_type
        # 83.39 populated, 0.03 unique
        #      Motor (48545)
        #      Diesel (24958)
        #      Dual Fuel (973)
        #      Common Rail (683)
        #      Electric Motors (510)
        json_data['propulsion_type'] = raw_data['propulsion_type']

        # columnName: class_1_code
        # 94.0 populated, 0.07 unique
        #      uu (10418)
        #      DNV-GL (10363)
        #      Nippon Kaiji (NK) (9439)
        #      American Bureau (AB) (5957)
        #      Lloyd's Register (LR) (5735)
        json_data['class_1_code'] = raw_data['class_1_code']

        # columnName: ice_class
        # 5.24 populated, 1.91 unique
        #      ICE Strengthening (673)
        #      GL - E3 (353)
        #      DNV-GL - Ice 1A (276)
        #      LR - 1A (230)
        #      CC - Ice Class B (229)
        json_data['ice_class'] = raw_data['ice_class']

        # columnName: ice_classed
        # 9.4 populated, 0.02 unique
        #      true (4803)
        #      false (3926)
        json_data['ice_classed'] = raw_data['ice_classed']

        # columnName: commercial_owner
        # 56.82 populated, 13.69 unique
        #      Unknown (4927)
        #      NYK (405)
        #      Mitsui O.S.K. (399)
        #      Moller A. P. (331)
        #      Unknown Chinese (317)
        json_data['commercial_owner'] = raw_data['commercial_owner']

        # columnName: built_year
        # 56.81 populated, 0.17 unique
        #      2010 (2794)
        #      2011 (2666)
        #      2009 (2589)
        #      2012 (2527)
        #      2008 (2498)
        json_data['built_year'] = raw_data['built_year']

        # columnName: dead_year
        # 0.07 populated, 19.4 unique
        #      2008 (34)
        #      2017 (8)
        #      2018 (7)
        #      2006 (3)
        #      2016 (3)
        json_data['dead_year'] = raw_data['dead_year']

        # columnName: vessel_age
        # 93.45 populated, 0.14 unique
        #      13 (3189)
        #      12 (3089)
        #      14 (3037)
        #      15 (3007)
        #      11 (2865)
        json_data['vessel_age'] = raw_data['vessel_age']

        # columnName: hull_number
        # 94.25 populated, 31.49 unique
        #      101 (127)
        #      102 (102)
        #      104 (97)
        #      105 (95)
        #      103 (94)
        json_data['hull_number'] = raw_data['hull_number']

        # columnName: ship_builder
        # 98.55 populated, 5.27 unique
        #      Hyundai Ulsan (1552)
        #      Hyundai Mipo (1349)
        #      Daewoo (1274)
        #      Samsung (1268)
        #      Imabari (1187)
        json_data['ship_builder'] = raw_data['ship_builder']

        # columnName: name_date
        # 36.01 populated, 77.44 unique
        #      2009-11-17 00:00:00 UTC (49)
        #      2021-10-21 00:00:00 UTC (46)
        #      2013-08-12 00:00:00 UTC (45)
        #      2009-11-16 00:00:00 UTC (43)
        #      2022-10-03 00:00:00 UTC (41)
        json_data['name_date'] = raw_data['name_date']

        # columnName: coated
        # 11.65 populated, 0.02 unique
        #      1 (8374)
        #      0 (2447)
        json_data['coated'] = raw_data['coated']

        # columnName: air_draught
        # 4.75 populated, 44.22 unique
        #      31.4 (48)
        #      31.5 (25)
        #      26 (23)
        #      40.86 (23)
        #      40 (21)
        json_data['air_draught'] = raw_data['air_draught']

        # columnName: draught
        # 93.0 populated, 8.87 unique
        #      4 (755)
        #      5 (728)
        #      14.5 (684)
        #      13.3 (671)
        #      6 (670)
        json_data['draught'] = raw_data['draught']

        # columnName: ktm
        # 7.3 populated, 25.46 unique
        #      50 (72)
        #      49 (65)
        #      46 (56)
        #      44 (56)
        #      47 (41)
        json_data['ktm'] = raw_data['ktm']

        # columnName: loa
        # 97.2 populated, 15.33 unique
        #      189.99 (1188)
        #      229 (1068)
        #      225 (998)
        #      199.9 (848)
        #      183 (683)
        json_data['loa'] = raw_data['loa']

        # columnName: trading_status
        # 100.0 populated, 0.01 unique
        #      Existing (71581)
        #      Scrapped (16090)
        #      NewBuilding (4021)
        #      TotalLoss (1100)
        #      Converting (42)
        json_data['trading_status'] = raw_data['trading_status']

        # columnName: trading_category
        # 81.47 populated, 0.0 unique
        #      In Service (71623)
        #      Newbuilding (4021)
        json_data['trading_category'] = raw_data['trading_category']

        # columnName: call_sign
        # 66.29 populated, 97.32 unique
        #      Unknown (5)
        #      PY2000 (4)
        #      3EWN5 (3)
        #      3EGC6 (3)
        #      3EGB2 (3)
        json_data['CALL_SIGN'] = raw_data['call_sign']

        # columnName: flag
        # 93.31 populated, 0.23 unique
        #      Panama (12619)
        #      Liberia (5957)
        #      China (4952)
        #      Marshall Islands (4581)
        #      Singapore (3662)
        json_data['flag'] = raw_data['flag']

        # columnName: group_owner
        # 54.68 populated, 12.91 unique
        #      COSCO (721)
        #      China Government (694)
        #      N.Y.K. Line (577)
        #      Mitsui O.S.K. (534)
        #      Moller, A. P. (488)
        json_data['group_owner'] = raw_data['group_owner']
        if raw_data['group_owner']:
            record_id = self.compute_record_hash(raw_data['group_owner'])
            json_data2 = {'DATA_SOURCE': json_data['DATA_SOURCE'],
                          'RECORD_ID': record_id,
                          'RECORD_TYPE': 'ORGANIZATION',
                          'NAME_ORG': raw_data['group_owner'],
                          'REL_ANCHOR_DOMAIN': json_data['DATA_SOURCE'],
                          'REL_ANCHOR_KEY': record_id}
            json_list.append(json_data2)
            json_data['REL_POINTER_DOMAIN'] = json_data['DATA_SOURCE']
            json_data['REL_POINTER_KEY'] = record_id
            json_data['REL_POINTER_ROLE'] = 'GROUP_OWNER'

        # columnName: beneficial_owner
        # 78.49 populated, 23.88 unique
        #      MSC (424)
        #      N.Y.K. Line (399)
        #      Mitsui O.S.K. (358)
        #      Moller, A. P. (341)
        #      Tidewater Inc. (341)
        json_data['beneficial_owner'] = raw_data['beneficial_owner']
        if raw_data['beneficial_owner']:
            record_id = self.compute_record_hash(raw_data['beneficial_owner'])
            json_data2 = {'DATA_SOURCE': json_data['DATA_SOURCE'],
                          'RECORD_ID': record_id,
                          'RECORD_TYPE': 'ORGANIZATION',
                          'NAME_ORG': raw_data['beneficial_owner'],
                          'REL_ANCHOR_DOMAIN': json_data['DATA_SOURCE'],
                          'REL_ANCHOR_KEY': record_id}
            json_list.append(json_data2)
            json_data['REL_POINTER_DOMAIN'] = json_data['DATA_SOURCE']
            json_data['REL_POINTER_KEY'] = record_id
            json_data['REL_POINTER_ROLE'] = 'BENEFICIAL_OWNER'

        # columnName: gear_type
        # 10.42 populated, 0.04 unique
        #      Crane (9208)
        #      Derrick (314)
        #      Gantry (94)
        #      Hose-handling crane (63)
        json_data['gear_type'] = raw_data['gear_type']

        # columnName: gear_quantity
        # 10.33 populated, 0.23 unique
        #      4 (3312)
        #      1 (2619)
        #      2 (2532)
        #      3 (987)
        #      5 (51)
        json_data['gear_quantity'] = raw_data['gear_quantity']

        # columnName: gear_model
        # 10.42 populated, 7.18 unique
        #      Generic Crane (2177)
        #      Generic Crane Gearless (1343)
        #      Generic Crane Undisclosed 30 (872)
        #      Generic Crane Undisclosed 30.5 (311)
        #      Generic Derrick (264)
        json_data['gear_model'] = raw_data['gear_model']

        # columnName: beam_extreme
        # 89.92 populated, 4.07 unique
        #      32.26 (5259)
        #      32.2 (2707)
        #      32.24 (1323)
        #      45 (1050)
        #      16 (990)
        json_data['beam_extreme'] = raw_data['beam_extreme']

        #--remove empty attributes and capture the stats
        json_data = self.remove_empty_tags(json_data)
        json_list.append(json_data)

        for json_data in json_list:
            self.capture_mapped_stats(json_data)


        return json_list

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']:
            return ''
        return new_value

    #-----------------------------------
    def compute_record_hash(self, target_dict, attr_list = None):
        if attr_list:
            string_to_hash = ''
            for attr_name in sorted(attr_list):
                string_to_hash += (' '.join(str(target_dict[attr_name]).split()).upper() if attr_name in target_dict and target_dict[attr_name] else '') + '|'
        else:
            string_to_hash = json.dumps(target_dict, sort_keys=True)
        return hashlib.md5(bytes(string_to_hash, 'utf-8')).hexdigest()

    #----------------------------------------
    def format_date(self, raw_date):
        try:
            return datetime.strftime(dateparse(raw_date), '%Y-%m-%d')
        except:
            self.update_stat('!INFO', 'BAD_DATE', raw_date)
            return ''

    #----------------------------------------
    def remove_empty_tags(self, d):
        if isinstance(d, dict):
            for  k, v in list(d.items()):
                if v is None or len(str(v).strip()) == 0:
                    del d[k]
                else:
                    self.remove_empty_tags(v)
        if isinstance(d, list):
            for v in d:
                self.remove_empty_tags(v)
        return d

    #----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example

    #----------------------------------------
    def capture_mapped_stats(self, json_data):

        cat1 = json_data.get('RECORD_TYPE', 'UNKNOWN')

        for key1 in json_data:
            if isinstance(json_data[key1], list):
                self.update_stat(cat1, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(cat1, key2, subrecord[key2])

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True

#----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False
    signal.signal(signal.SIGINT, signal_handler)

    input_file = 'enhanced_vessel_information.csv'
    csv_dialect = 'excel'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', dest='input_file', default = input_file, help='the name of the input file')
    parser.add_argument('-o', '--output_file', dest='output_file', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)
    if not args.output_file:
        print('\nPlease supply a valid output file name on the command line\n')
        sys.exit(1)

    input_file_handle = open(args.input_file, 'r')
    output_file_handle = open(args.output_file, 'w', encoding='utf-8')
    mapper = mapper()

    input_row_count = 0
    output_row_count = 0
    for input_row in csv.DictReader(input_file_handle, dialect=csv_dialect):
        input_row_count += 1

        json_list = mapper.map(input_row, input_row_count)
        for json_data in json_list:
            output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

        if input_row_count % 1000 == 0:
            print('%s rows processed, %s rows written' % (input_row_count, output_row_count))
        if shut_down:
            break

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print('%s rows processed, %s rows written, %s\n' % (input_row_count, output_row_count, run_status))

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file:
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)

    sys.exit(0)
