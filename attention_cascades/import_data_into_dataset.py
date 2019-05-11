#!/usr/bin/python
"""
Author: Josemar Caetano
"""
import sys
import os
from datetime import datetime

from configparser import ConfigParser

import time
import json

import phonenumbers
from phonenumbers.phonenumberutil import region_code_for_number

import re

from langid.langid import LanguageIdentifier, model

import sqlite3

import collections


class DataLoading:
    def __init__(self, black_listed_groups_ids):
        self.black_listed_groups_ids = black_listed_groups_ids


    def buildDatabasesTSVs(self, files_directories_filename):
        try:
            databases = {'msgstore':['chat_list', 'group_participants', 'group_participants_history', 'messages', 'messages_quotes'],
                         "wa": ['wa_contacts', 'wa_group_descriptions', 'wa_vnames']}

            #databases = {'msgstore': ['messages_quotes']}

            with open(files_directories_filename, 'r', encoding='utf-8') as f:
                info = f.read().splitlines()
                for directoryAndDeviceInfo in info:
                    print(">>>>>>>>>>>>>>>", directoryAndDeviceInfo, flush=True)
                    databases_processed = []
                    valid_directory = directoryAndDeviceInfo.split(";")[0]
                    directory = directoryAndDeviceInfo.split(";")[1]
                    device = directoryAndDeviceInfo.split(";")[2]

                    #print("Valid directory", valid_directory, "\tDirectory:",directory, "\tDevice", device, flush=True)

                    if(str(valid_directory) == "YES"):
                        data_folder = str(directory+ "/SQLite/")
                        for dirname, dirnames, filenames in os.walk(data_folder):
                            for filename in filenames:
                                database_name = filename.split('.')[0]

                                if(database_name in databases and database_name not in databases_processed):
                                    databases_processed.append(database_name)
                                    tables_names = databases[database_name]
                                    for table_name in tables_names:
                                        print("Extracting SQLite data from device:", device, "\tTable:",table_name,'\tDatetime: ', datetime.now(),
                                              flush=True)
                                        data_folder_tsv = str(directory+ "/TSV/") + table_name + ".tsv"  # output filename.

                                        sqlite_file = data_folder + database_name + ".db"

                                        connection = sqlite3.connect(sqlite_file)  # open connection to your database
                                        cursor = connection.cursor()  # get a cursor for it

                                        with open(data_folder_tsv, "w", encoding='utf-8') as out_file:
                                            cursor.execute("PRAGMA table_info(" + table_name + ");")  # execute the query
                                            rows = cursor.fetchall()  # collect the data
                                            header_str = ""
                                            for row in rows:
                                                field_name = row[1]
                                                header_str += str(field_name) + "\t"

                                            out_file.write(header_str + "\n")

                                            cursor.execute("SELECT * FROM " + table_name)  # execute the query
                                            rows = cursor.fetchall()  # collect the data
                                            row_index = 0

                                            for row in rows:

                                                row_index +=1

                                                if(row_index % 10000 == 0):
                                                    print("Processing document index", row_index)

                                                tmp_str = ""
                                                for val in row:
                                                    if (val is None):
                                                        my_val = ""
                                                    else:
                                                        my_val = str(val).rstrip("\n").rstrip("\t").rstrip("\r")
                                                        my_val = str(my_val).replace('\r', " ").replace('\n', " ").replace('\t', " ")
                                                    tmp_str += str(my_val) + "\t"

                                                out_file.write(tmp_str + "\n")

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)

    def convertLineToTempDict(self, line, headers):
        try:
            line = str(line).replace("\t\n", "")
            fields = line.split("\t")
            if(len(fields) == len(headers)):
                temp_message = {}

                for head, field in zip(headers, fields):
                    temp_message[head] = field

                return(temp_message)
            else:
                return(None)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)

    def isValidGroup(self, group_id, selected_groups):
        try:
            if(len(group_id.split("-")) == 2 and (selected_groups is None or group_id in selected_groups) and str(group_id)[0:2] == "55" and group_id not in self.black_listed_groups_ids):
                return True
            else:
                return False

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)

    def isValidMessage(self, message_id):
        try:
            if(message_id != "" and message_id != -1):
                return True
            else:
                return False

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)


    def processGroupsCollection(self, filename_original_directories, selected_groups, filename_groups):
        groups_inserted_set = set()
        if(os.path.exists(filename_groups) is True):
            with open(filename_groups, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    groups_inserted_set.add(group["_id"])
        try:
            with open(filename_original_directories, 'r', encoding='utf-8') as f:
                info = f.read().splitlines()
                for directoryAndDeviceInfo in info:
                    valid_directory = directoryAndDeviceInfo.split(";")[0]
                    directory = directoryAndDeviceInfo.split(";")[1]
                    device = directoryAndDeviceInfo.split(";")[2]
                    import_date = directoryAndDeviceInfo.split(";")[3]

                    if (str(valid_directory) == "YES"):
                        data_folder = str(directory + "/TSV/")

                        for dirname, dirnames, filenames in os.walk(data_folder):
                            for filename in filenames:
                                complete_filename = str(os.path.join(dirname, filename))

                                collection_name = filename.split('.')[0]

                                if(collection_name == "chat_list"):
                                    print("Processing data from device:", device, "\tCollection name: ",
                                          collection_name, '\tDatetime: ', datetime.now(),
                                          flush=True)
                                    with open(filename_groups, 'a', encoding='utf-8') as file_groups:
                                        with open(complete_filename, 'r', encoding='utf-8') as file:
                                            headers = str(file.readline()).replace("\t\n","").split("\t")
                                            for line in file:
                                                temp_group = self.convertLineToTempDict(line=line, headers=headers)

                                                if (temp_group is not None):
                                                    valid_group = self.isValidGroup(temp_group['key_remote_jid'],
                                                                                    selected_groups=selected_groups)

                                                    if (valid_group is True and temp_group['creation'] != "" and
                                                                temp_group['key_remote_jid'] not in groups_inserted_set):
                                                        group = {}
                                                        group['DEVICE'] = device
                                                        group['IMPORT_DATE'] = import_date
                                                        group['_id'] = temp_group['key_remote_jid']
                                                        group['created_by'] = \
                                                            str(temp_group['key_remote_jid'].split("-")[0] +"@s.whatsapp.net")

                                                        try:
                                                            epoch_time = str(temp_group['creation'])[0: 10]
                                                            group['creation'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                              time.localtime(
                                                                                                  int(epoch_time)))
                                                        except Exception as e:
                                                            print(temp_group)

                                                        group['name'] = temp_group['subject']


                                                        phone_number = group['created_by'].split("@")[0]
                                                        nationality_info = phonenumbers.parse(str("+" + phone_number),
                                                                                              None)

                                                        country_phone_code = nationality_info.country_code
                                                        international_country_code = region_code_for_number(
                                                            nationality_info)

                                                        group['nationality_info'] = {
                                                            "country_phone_code": str(
                                                                country_phone_code),
                                                            "international_country_code": str(
                                                                international_country_code)}

                                                        json.dump(group, file_groups)
                                                        file_groups.write('\n')

                                                        groups_inserted_set.add(group['_id'])

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)


    def processUsersAndGroupsUsersCollection(self, original_files_directories_filename, selected_groups,
                                             filename_groups, filename_groups_users, filename_users):
        users_inserted_set = set()
        if (os.path.exists(filename_users) is True):
            with open(filename_users, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    users_inserted_set.add(group["_id"])

        groups_users_inserted_set = set()
        if (os.path.exists(filename_groups_users) is True):
            with open(filename_groups_users, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    groups_users_inserted_set.add(group["_id"])

        available_groups = set()
        if (os.path.exists(filename_groups) is True):
            with open(filename_groups, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    available_groups.add(group["_id"])

        try:
            with open(original_files_directories_filename, 'r', encoding='utf-8') as f:
                info = f.read().splitlines()
                for directoryAndDeviceInfo in info:
                    valid_directory = directoryAndDeviceInfo.split(";")[0]
                    directory = directoryAndDeviceInfo.split(";")[1]
                    device = directoryAndDeviceInfo.split(";")[2]

                    if (str(valid_directory) == "YES"):
                        data_folder = str(directory + "/TSV/")

                        for dirname, dirnames, filenames in os.walk(data_folder):
                            for filename in filenames:
                                complete_filename = str(os.path.join(dirname, filename))

                                collection_name = filename.split('.')[0]
                                # Insert enrolled users
                                if(collection_name == "group_participants"):
                                    print("\tProcessing data from device:", device, "\tCollection name: ",
                                          collection_name, '\tDatetime: ', datetime.now(),
                                          flush=True)
                                    with open(filename_users, 'a', encoding='utf-8') as file_users:
                                        with open(filename_groups_users, 'a', encoding='utf-8') as file_groups_users:
                                            with open(complete_filename, 'r', encoding='utf-8') as file:
                                                headers = str(file.readline()).replace("\t\n", "").split("\t")
                                                for line in file:
                                                    temp_group_participant = self.convertLineToTempDict(line=line, headers=headers)

                                                    if (temp_group_participant is not None and temp_group_participant['gjid'] != "" and  temp_group_participant['jid'] != ""):
                                                        groups_users_inserted_set, users_inserted_set = self.processUsersAndGroupsUsersDocument(collection_name=collection_name,
                                                                                           temp_document=temp_group_participant,
                                                                                           groups_users_inserted_set=groups_users_inserted_set,
                                                                                           file_groups_users=file_groups_users, device=device,
                                                                                           users_inserted_set=users_inserted_set, file_users=file_users,
                                                                                        selected_groups=selected_groups,available_groups=available_groups)
                            for dirname, dirnames, filenames in os.walk(data_folder):
                                for filename in filenames:
                                    complete_filename = str(os.path.join(dirname, filename))

                                    collection_name = filename.split('.')[0]
                                    # Insert active users
                                    if(collection_name == "messages"):
                                        print("\tProcessing data from device:", device, "\tCollection name: ",
                                              collection_name, '\tDatetime: ', datetime.now(),
                                              flush=True)
                                        with open(filename_users, 'a', encoding='utf-8') as file_users:
                                            with open(filename_groups_users, 'a', encoding='utf-8') as file_groups_users:
                                                with open(complete_filename, 'r', encoding='utf-8') as file:
                                                    headers = str(file.readline()).replace("\t\n", "").split("\t")
                                                    for line in file:
                                                        temp_message = self.convertLineToTempDict(line=line,
                                                                                                  headers=headers)

                                                        if (temp_message is not None and temp_message['key_id'] != "" and temp_message[
                                                                'key_id'] != -1 and
                                                                    ((temp_message['data'] != "" and temp_message[
                                                                        'media_mime_type'] == "") or
                                                                         (temp_message['data'] == "" and temp_message[
                                                                             'media_mime_type'] != "")) and temp_message['key_from_me'] == "0"):
                                                            groups_users_inserted_set, users_inserted_set = self.processUsersAndGroupsUsersDocument(
                                                                collection_name=collection_name,
                                                                temp_document=temp_message,
                                                                groups_users_inserted_set=groups_users_inserted_set,
                                                                file_groups_users=file_groups_users, device=device,
                                                                users_inserted_set=users_inserted_set,
                                                                file_users=file_users, selected_groups=selected_groups, available_groups=available_groups)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)

    def processUsersAndGroupsUsersDocument(self, collection_name, temp_document, groups_users_inserted_set,
                                           file_groups_users, device, users_inserted_set, file_users, selected_groups,
                                           available_groups):
        try:
            ## Common features
            if (collection_name == 'group_participants'):
                valid_group = self.isValidGroup(
                    group_id=temp_document['gjid'], selected_groups=selected_groups)
            else:
                valid_group = self.isValidGroup(
                    group_id=temp_document['key_remote_jid'], selected_groups=selected_groups)


            if valid_group is True:
                common_document = {}
                if(collection_name == 'group_participants'):
                    common_document['group_id'] = temp_document['gjid']
                    common_document['user_id'] = temp_document['jid']
                else:
                    common_document['group_id'] = temp_document['key_remote_jid']
                    common_document['user_id'] = temp_document['remote_resource']

                if(common_document['group_id'] in available_groups):
                    common_document['user_number'] = \
                        common_document['user_id'].split('@')[0]
                    common_document["DEVICE"] = device

                    phone_number = common_document['user_number']
                    #nationality_info = None
                    nationality_info = phonenumbers.parse(str("+" + phone_number),
                                                              None)


                    country_phone_code = nationality_info.country_code
                    international_country_code = region_code_for_number(
                        nationality_info)

                    common_document['nationality_info'] = {
                        "country_phone_code": str(
                            country_phone_code),
                        "international_country_code": str(
                            international_country_code)}

                    # # ===================>> PROCESSING GROUPS_USERS
                    group_user_id = common_document['group_id'] + '/' + \
                                    common_document['user_id']
                    if (group_user_id not in groups_users_inserted_set):
                        group_user = common_document
                        group_user['_id'] = group_user_id

                        if (collection_name == 'group_participants'):
                            group_user['is_admin'] = "0" if temp_document[
                                                                'admin'] == "0" else "1"
                        else:
                            group_user['is_admin'] = "undefined"

                        json.dump(group_user, file_groups_users)
                        file_groups_users.write('\n')

                        groups_users_inserted_set.add(group_user['_id'])

                    # # ===================>> PROCESSING USERS
                    if (common_document['user_id'] not in users_inserted_set):
                        user = common_document
                        user['_id'] = user['user_id']

                        json.dump(user, file_users)
                        file_users.write('\n')

                        users_inserted_set.add(user['_id'])

            return(groups_users_inserted_set, users_inserted_set)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)


    def processMessagesCollection(self, original_files_directories_filename, selected_groups,
                                  filename_messages, filename_control_messages, filename_groups, filename_users,
                                  filename_messages_quotes):
        total_users_messages = 0
        all_total_messages = 0
        total_control_messages = 0
        total_users_messages_quotes = 0

        print("\t>>>>> Loading already inserted messages ids", flush=True)
        messages_inserted_set = set()
        if (os.path.exists(filename_messages) is True):
            with open(filename_messages, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    messages_inserted_set.add(group["_id"])

        print("\t>>>>> Loading already inserted control messages ids", flush=True)
        control_messages_inserted_set = set()
        if (os.path.exists(filename_control_messages) is True):
            with open(filename_control_messages, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    control_messages_inserted_set.add(group["_id"])

        print("\t>>>>> Loading already inserted messages quotes ids", flush=True)
        messages_quotes_inserted_set = set()
        if (os.path.exists(filename_messages_quotes) is True):
            with open(filename_messages_quotes, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    messages_quotes_inserted_set.add(group["_id"])

        print("\t>>>>> Initiating insertion process", flush=True)
        try:
            groups = {}
            users = {}

            if (os.path.exists(filename_groups) is True):
                with open(filename_groups, 'r', encoding='utf-8') as file:
                    for group in file:
                        group = json.loads(group)
                        groups[group['_id']] = group

            if (os.path.exists(filename_users) is True):
                with open(filename_users, 'r', encoding='utf-8') as file:
                    for user in file:
                        user = json.loads(user)
                        users[user['_id']] = user

            identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)

            with open(original_files_directories_filename, encoding='utf-8') as f:
                info = f.read().splitlines()
                for directoryAndDeviceInfo in info:
                    valid_directory = directoryAndDeviceInfo.split(";")[0]
                    directory = directoryAndDeviceInfo.split(";")[1]
                    device = directoryAndDeviceInfo.split(";")[2]
                    import_date = directoryAndDeviceInfo.split(";")[3]

                    if (str(valid_directory) == "YES"):

                        data_folder = str(directory + "/TSV/")

                        for dirname, dirnames, filenames in os.walk(data_folder):
                            for filename in filenames:
                                complete_filename = str(os.path.join(dirname, filename))

                                collection_name = filename.split('.')[0]

                                if (collection_name == "messages"):
                                    print("Processing data from device:", device, "\tDirectory:", directory,
                                          "\tCollection name: ", collection_name, '\tDatetime: ', datetime.now(),
                                          flush=True)
                                    with open(filename_control_messages, 'a', encoding='utf-8') as file_control_messages:
                                        with open(filename_messages_quotes, 'a',
                                                  encoding='utf-8') as file_messages_quotes:
                                            with open(filename_messages, 'a', encoding='utf-8') as file_messages:
                                                with open(complete_filename, 'r', encoding='utf-8') as file:
                                                    headers = str(file.readline()).replace("\t\n", "").split("\t")
                                                    for line in file:
                                                        temp_message = self.convertLineToTempDict(line=line, headers=headers)
                                                        if(temp_message is not None):
                                                            all_total_messages += 1
                                                            if(all_total_messages % 100000 == 0):
                                                                print("\t >>>>> Processed so far", all_total_messages,'\tDatetime: ', datetime.now(), flush=True)

                                                            valid_group = self.isValidGroup(temp_message['key_remote_jid'],
                                                                                            selected_groups=selected_groups)

                                                            valid_message = self.isValidMessage(temp_message['key_id'])

                                                            is_text_message = (temp_message['data'] != "" and
                                                                                  temp_message['media_mime_type'] == "")

                                                            is_media_message = (temp_message['data'] == "" and
                                                                                      temp_message['media_mime_type'] != "")


                                                            # Check if it is a valid data message (text or media)
                                                            if (valid_group is True and valid_message and
                                                                temp_message['key_remote_jid'] in groups and
                                                                temp_message['remote_resource'] in users and
                                                                ((temp_message['status'] == "0" and
                                                                          temp_message['key_id'] not in messages_inserted_set and
                                                                      (is_text_message or is_media_message)) or
                                                                     (temp_message['status'] != "0" and temp_message['key_id'] not in control_messages_inserted_set))
                                                                ):

                                                                message = {}
                                                                message['DEVICE'] = device
                                                                message['IMPORT_DATE'] = import_date
                                                                message['_id'] = temp_message['key_id']
                                                                message['group'] = groups[temp_message['key_remote_jid']]
                                                                message['user'] = users[temp_message['remote_resource']]

                                                                message['text'] = temp_message['data']
                                                                epoch_time = str(temp_message['timestamp'])[0: 10]
                                                                message['publication_date'] = time.strftime('%Y-%m-%d %H:%M:%S',
                                                                                                            time.localtime(int(
                                                                                                                epoch_time)))

                                                                epoch_time = str(temp_message['received_timestamp'])[0: 10]
                                                                message['received_timestamp'] = time.strftime(
                                                                    '%Y-%m-%d %H:%M:%S', time.localtime(int(epoch_time)))

                                                                type = "text"
                                                                if (len(temp_message['media_mime_type'].split("/")) > 1):
                                                                    type = temp_message['media_mime_type'].split("/")[0]

                                                                message['status'] = temp_message['status']

                                                                message['type_detailed'] = temp_message['media_mime_type']
                                                                message['type'] = type

                                                                message['media_info'] = {'media_size': temp_message['media_size'],
                                                                'media_duration': temp_message['media_duration'],
                                                                'media_name': temp_message['media_name'],
                                                                'media_caption': temp_message['media_caption'],
                                                                'media_hash': temp_message['media_hash'],
                                                                'media_enc_hash': temp_message['media_enc_hash'],
                                                                #'media_info_dump': temp_message['thumb_image'],
                                                                'media_url': temp_message['media_url']
                                                                }

                                                                message['recipient_count'] = temp_message['recipient_count']
                                                                message['latitude'] = temp_message['latitude']
                                                                message['longitude'] = temp_message['longitude']

                                                                message['quoted_row_id'] = temp_message['quoted_row_id']

                                                                message['is_forwarded'] = "NO-INFORMATION"

                                                                try:
                                                                    message['is_forwarded'] = temp_message['forwarded']
                                                                except Exception as e:
                                                                    #print("\tMessage without 'forwarded' information")
                                                                    pass

                                                                message['raw_data'] = ""
                                                                if ('raw_data' in temp_message):
                                                                    message['raw_data'] = temp_message['raw_data']

                                                                message['special_type'] = ""
                                                                if (message['latitude'] != "" and message['longitude'] != "" and
                                                                            message['raw_data'] != ""):
                                                                    message['special_type'] = "Geolocation Coordinates"
                                                                if (temp_message['media_wa_type'] == 4):
                                                                    message['special_type'] = "Contact Card"

                                                                if(message['type'] == 'text'):
                                                                    langid_language_detected_result = identifier.classify(message['text'])
                                                                    langid_language_detected = langid_language_detected_result[0]
                                                                    language_probability = langid_language_detected_result[1]

                                                                    if (langid_language_detected is None):
                                                                        langid_language_detected = ""
                                                                    if (language_probability is None):
                                                                        language_probability = ""

                                                                    message['text_language_info'] = {"langid_language_detected": langid_language_detected, "language_probability":language_probability}

                                                                if(message['status'] == '0'):
                                                                    json.dump(message, file_messages)
                                                                    file_messages.write('\n')

                                                                    messages_inserted_set.add(message['_id'])

                                                                    total_users_messages +=1

                                                                else:
                                                                    json.dump(message, file_control_messages)
                                                                    file_control_messages.write('\n')

                                                                    control_messages_inserted_set.add(message['_id'])

                                                                    total_control_messages +=1

                                                                if (temp_message['quoted_row_id'] != "" and
                                                                            temp_message['quoted_row_id'] is not None and
                                                                            temp_message['quoted_row_id'] != "0" and
                                                                        message['_id'] not in messages_quotes_inserted_set):
                                                                    message_quote = {}
                                                                    message_quote['DEVICE'] = device
                                                                    message_quote['IMPORT_DATE'] = import_date

                                                                    message_quote['_id'] = message['_id']
                                                                    message_quote['group'] = message['group']
                                                                    message_quote['user'] = message['user']

                                                                    message_quote['publication_date'] = message['publication_date']
                                                                    message_quote['received_timestamp'] = message[
                                                                        'received_timestamp']

                                                                    message_quote['quoted_row_id'] = temp_message['quoted_row_id']

                                                                    json.dump(message_quote, file_messages_quotes)
                                                                    file_messages_quotes.write('\n')

                                                                    messages_quotes_inserted_set.add(message_quote['_id'])

                                                                    total_users_messages_quotes += 1

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)
        finally:
            print("Total messages:", all_total_messages, "\tTotal user messages:", total_users_messages,
                  "\tTotal control messages:", total_control_messages)


    def processMessagesQuotesCollection(self, original_files_directories_filename, selected_groups,
                                        filename_messages_quotes_references, filename_groups, filename_users):
        total_users_messages_quotes = 0
        all_total_messages_qutoes = 0

        print("\t>>>>> Loading already inserted messages quotes ids", flush=True)
        messages_quotes_inserted_set = set()
        if (os.path.exists(filename_messages_quotes_references) is True):
            with open(filename_messages_quotes_references, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    messages_quotes_inserted_set.add(group["_id"])

        print("\t>>>>> Initiating insertion process", flush=True)
        try:
            groups = {}
            users = {}

            if (os.path.exists(filename_groups) is True):
                with open(filename_groups, 'r', encoding='utf-8') as file:
                    for group in file:
                        group = json.loads(group)
                        groups[group['_id']] = group

            if (os.path.exists(filename_users) is True):
                with open(filename_users, 'r', encoding='utf-8') as file:
                    for user in file:
                        user = json.loads(user)
                        users[user['_id']] = user

            with open(original_files_directories_filename, encoding='utf-8') as f:
                info = f.read().splitlines()
                for directoryAndDeviceInfo in info:
                    valid_directory = directoryAndDeviceInfo.split(";")[0]
                    directory = directoryAndDeviceInfo.split(";")[1]
                    device = directoryAndDeviceInfo.split(";")[2]
                    import_date = directoryAndDeviceInfo.split(";")[3]

                    if (str(valid_directory) == "YES"):

                        data_folder = str(directory + "/TSV/")

                        for dirname, dirnames, filenames in os.walk(data_folder):
                            for filename in filenames:
                                complete_filename = str(os.path.join(dirname, filename))

                                collection_name = filename.split('.')[0]

                                if (collection_name == "messages_quotes"):
                                    print("Processing data from device:", device, "\tCollection name: ",
                                          collection_name, '\tDatetime: ', datetime.now(),
                                          flush=True)
                                    with open(filename_messages_quotes_references, 'a', encoding='utf-8') as file_messages_quotes_references:
                                        with open(complete_filename, 'r', encoding='utf-8') as file:
                                            headers = str(file.readline()).replace("\t\n", "").split("\t")
                                            for line in file:
                                                temp_message_quote_reference = self.convertLineToTempDict(line=line,
                                                                                                headers=headers)
                                                if (temp_message_quote_reference is not None):
                                                    all_total_messages_qutoes += 1
                                                    if (all_total_messages_qutoes % 10000 == 0):
                                                        print("Processed so far", all_total_messages_qutoes)

                                                    valid_group = self.isValidGroup(
                                                        temp_message_quote_reference['key_remote_jid'],
                                                        selected_groups=selected_groups)

                                                    if (valid_group is True and temp_message_quote_reference['key_id'] != "" and
                                                                temp_message_quote_reference['key_id'] != -1 and
                                                                temp_message_quote_reference[
                                                                    'key_id'] not in messages_quotes_inserted_set and
                                                                temp_message_quote_reference['key_remote_jid'] in groups and
                                                                temp_message_quote_reference['remote_resource'] in users):

                                                        message_quote_reference = {}
                                                        message_quote_reference['DEVICE'] = device
                                                        message_quote_reference['IMPORT_DATE'] = import_date
                                                        message_quote_reference['_id'] = \
                                                            str(temp_message_quote_reference['key_id']+"/"+
                                                                temp_message_quote_reference['_id']+"/"+str(temp_message_quote_reference['timestamp']))
                                                        message_quote_reference['group'] = groups[
                                                            temp_message_quote_reference['key_remote_jid']]
                                                        message_quote_reference['user'] = users[
                                                            temp_message_quote_reference['remote_resource']]

                                                        epoch_time = str(temp_message_quote_reference['timestamp'])[0: 10]
                                                        message_quote_reference['publication_date'] = time.strftime(
                                                            '%Y-%m-%d %H:%M:%S',
                                                            time.localtime(int(
                                                                epoch_time)))

                                                        epoch_time = str(temp_message_quote_reference['received_timestamp'])[
                                                                     0: 10]
                                                        message_quote_reference['received_timestamp'] = time.strftime(
                                                            '%Y-%m-%d %H:%M:%S', time.localtime(int(epoch_time)))


                                                        message_quote_reference['quoted_row_id'] = temp_message_quote_reference['_id']
                                                        message_quote_reference['quoted_message_id'] = \
                                                        temp_message_quote_reference['key_id']

                                                        json.dump(message_quote_reference, file_messages_quotes_references)
                                                        file_messages_quotes_references.write('\n')

                                                        messages_quotes_inserted_set.add(message_quote_reference['_id'])

                                                        total_users_messages_quotes += 1

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)
        finally:
            print("Total messages:", all_total_messages_qutoes, "\tTotal quote messages:", total_users_messages_quotes)


def main():
    try:
        start_time = datetime.now()

        DATA_DIRECTORY = "/home/<<user>>/data/"

        # SOURCE FILES FILENAMES
        filename_messages = str(DATA_DIRECTORY+"source_files/messages.jsonl")
        filename_quote_messages = str(DATA_DIRECTORY+"source_files/quote_messages.jsonl")
        filename_control_messages = str(DATA_DIRECTORY+"source_files/control_messages.jsonl")
        filename_groups = str(DATA_DIRECTORY+"source_files/groups.jsonl")
        filename_users = str(DATA_DIRECTORY+"source_files/users.jsonl")
        filename_groups_users = str(DATA_DIRECTORY+"source_files/groups_users.jsonl")
        filename_messages_quotes_references = str(DATA_DIRECTORY+"source_files/messages_quotes_references.jsonl")
        filename_black_listed_groups_ids =str(DATA_DIRECTORY+"source_files/groups_black_listed.txt")
        filename_original_directories = str(DATA_DIRECTORY+"db_source_files/databases_import_info.txt")

        # >>>>>>>>>>>>>>>>>>>>>>>
        black_listed_groups_ids = []
        with open(filename_black_listed_groups_ids, 'r', encoding='utf-8') as file:
            for line in file:
                a_group_id = line.replace("\n", "")
                black_listed_groups_ids.append(a_group_id)

        dataLoading = DataLoading(black_listed_groups_ids=black_listed_groups_ids)

        selected_groups = None
        ##selected_groups = dataLoading.getSelectedGroupsSet()

        print(">>>>>>>> Running dataLoading.buildDatabasesTSVs method", flush=True)
        dataLoading.buildDatabasesTSVs(files_directories_filename=filename_original_directories)


        print(">>>>>>>> Running dataLoading.processGroupsCollection method", flush=True)
        dataLoading.processGroupsCollection(filename_original_directories=filename_original_directories,
                                            selected_groups=selected_groups,
                                            filename_groups=filename_groups)

        print(">>>>>>>> Running dataLoading.processUsersAndGroupsUsersCollection method", flush=True)
        dataLoading.processUsersAndGroupsUsersCollection(original_files_directories_filename=filename_original_directories,
                                                         selected_groups=selected_groups, filename_groups=filename_groups,
                                                         filename_groups_users=filename_groups_users,
                                                         filename_users=filename_users)

        print(">>>>>>>> Running dataLoading.processMessagesCollection method", flush=True)
        dataLoading.processMessagesCollection(original_files_directories_filename=filename_original_directories, selected_groups=selected_groups,
                                              filename_messages=filename_messages, filename_control_messages=filename_control_messages,
                                              filename_groups=filename_groups, filename_users=filename_users,
                                              filename_messages_quotes=filename_quote_messages)

        print(">>>>>>>> Running dataLoading.processMessagesQuotesCollection method", flush=True)
        dataLoading.processMessagesQuotesCollection(
            original_files_directories_filename=filename_original_directories, selected_groups=selected_groups,
            filename_messages_quotes_references=filename_messages_quotes_references,
            filename_groups=filename_groups, filename_users=filename_users)


        end_time = datetime.now()
        print("\nStart time: %s\nFinal time: %s\nTime elapsed (seconds): %s\n" % (
            start_time, end_time, (end_time - start_time).seconds))

        print("\n\n###########################################################\tSCRIPT FINISHED.", flush=True)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
              flush=True)

if __name__ == '__main__':
    #pass
    main()
