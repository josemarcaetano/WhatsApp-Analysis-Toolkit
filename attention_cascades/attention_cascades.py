#!/usr/bin/python
"""
Author: Josemar Caetano
"""
import sys
import os
from datetime import datetime

import json


from operator import itemgetter

import networkx as nx

import operator

from collections import OrderedDict

import re, math
from collections import Counter

import itertools


class AttentionCascades:
    def __init__(self):
        pass

    def getDateFormatted(self, string_datetime):
        a_datetime = None
        string_datetime = string_datetime.replace("\"","")
        string_datetime = string_datetime.replace("'", "")
        try:
            a_datetime = datetime.strptime(string_datetime, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            try:
                a_datetime = datetime.strptime(string_datetime, '%Y-%m-%dT%H:%M:%SZ')
            except Exception as e:
                print("ERROR on formattring datetime:",a_datetime, flush=True)

        return(a_datetime)

    def buildDialoguesCollection(self, filename_messages_dialogues, filename_messages, filename_quote_messages,
                                 filename_messages_quotes_references):
        total_users_messages_quotes = 0
        all_total_messages_quotes = 0

        print("\t>>>>> Loading already inserted messages quotes ids", flush=True)
        messages_dialogues_inserted_set = set()
        if (os.path.exists(filename_messages_dialogues) is True):
            with open(filename_messages_dialogues, 'r', encoding='utf-8') as file_groups:
                for group in file_groups:
                    group = json.loads(group)
                    messages_dialogues_inserted_set.add(group["_id"])

        print("\t>>>>> Building reference hash of messages", flush=True)
        original_messages_hash = {}
        line_number = 0
        with open(filename_messages, 'r',
                  encoding='utf-8') as file_messages:
            for message in file_messages:
                message = json.loads(message)

                line_number += 1
                if (line_number % 100000 == 0):
                    print("\t\tOriginal messages hashed so far", line_number, '\tDatetime: ', datetime.now(),
                          flush=True)

                original_messages_hash[message['_id']] = message

        print("\t>>>>> Building reference hash of quote_messages", flush=True)
        quote_messages_hash = {}
        line_number = 0
        with open(filename_quote_messages, 'r',
                  encoding='utf-8') as file_quote_messages:
            for quote_message in file_quote_messages:
                quote_message = json.loads(quote_message)

                line_number += 1

                if (line_number % 100000 == 0):
                    print("\t\tQuote messages hashed so far", line_number, '\tDatetime: ', datetime.now(),
                          flush=True)

                if (quote_message['DEVICE'] not in quote_messages_hash):
                    quote_messages_hash[quote_message['DEVICE']] = {}

                if (quote_message['IMPORT_DATE'] not in quote_messages_hash[quote_message['DEVICE']]):
                    quote_messages_hash[quote_message['DEVICE']][quote_message['IMPORT_DATE']] = {}

                if (quote_message['group']['_id'] not in quote_messages_hash[quote_message['DEVICE']][quote_message['IMPORT_DATE']]):
                    quote_messages_hash[quote_message['DEVICE']][quote_message['IMPORT_DATE']][quote_message['group']['_id']] = {}

                if (quote_message['quoted_row_id'] not in quote_messages_hash[quote_message['DEVICE']][quote_message['IMPORT_DATE']][quote_message['group']['_id']]):
                    quote_messages_hash[quote_message['DEVICE']][quote_message['IMPORT_DATE']][
                        quote_message['group']['_id']][quote_message['quoted_row_id']] = []

                # Get complete message
                if(quote_message['_id'] in original_messages_hash):
                    complete_quote_message = original_messages_hash[quote_message['_id']]
                    complete_quote_message['quoted_row_id'] = quote_message['quoted_row_id']

                    quote_messages_hash[quote_message['DEVICE']][quote_message['IMPORT_DATE']][quote_message['group']['_id']][quote_message['quoted_row_id']].append(complete_quote_message)

        print("\t>>>>> Initiating insertion process", flush=True)
        try:
            with open(filename_messages_dialogues, 'a', encoding='utf-8') as file_messages_dialogues:
                with open(filename_messages_quotes_references, 'r',
                          encoding='utf-8') as file_messages_quotes_references:
                    for message_quoted_reference in file_messages_quotes_references:
                        message_quoted_reference = json.loads(message_quoted_reference)

                        all_total_messages_quotes += 1
                        if (all_total_messages_quotes % 100000 == 0):
                            print("Dialogues processed so far", all_total_messages_quotes, '\tDatetime: ',
                                  datetime.now(),
                                  flush=True)

                        if (message_quoted_reference['DEVICE'] in quote_messages_hash and
                                    message_quoted_reference['IMPORT_DATE'] in quote_messages_hash[message_quoted_reference['DEVICE']] and
                                    message_quoted_reference['group']['_id'] in quote_messages_hash[message_quoted_reference['DEVICE']][
                                    message_quoted_reference['IMPORT_DATE']]):
                            quote_info = quote_messages_hash[message_quoted_reference['DEVICE']][message_quoted_reference['IMPORT_DATE']][message_quoted_reference['group']['_id']]
                            found_quote_message = False
                            quote_message = None
                            if (message_quoted_reference['quoted_row_id'] in quote_info):
                                for quote_message in quote_info[message_quoted_reference['quoted_row_id']]:
                                    if (quote_message['quoted_row_id'] == message_quoted_reference['quoted_row_id']):
                                        found_quote_message = True
                                        break
                                if (found_quote_message is True and quote_message is not None and message_quoted_reference['quoted_message_id'] in original_messages_hash):
                                    # Check if the quote message was published after the original message (SANITY TEST)
                                    publication_date_quoted_message = self.getDateFormatted(string_datetime=original_messages_hash[message_quoted_reference['quoted_message_id']]['publication_date'])
                                    publication_date_quote_message = self.getDateFormatted(string_datetime=quote_message['publication_date'])

                                    if(publication_date_quote_message > publication_date_quoted_message):
                                        message_dialogue_id = str(message_quoted_reference['quoted_message_id'] + "/" +quote_message['_id'])

                                        if(message_dialogue_id not in messages_dialogues_inserted_set):
                                            original_message = original_messages_hash[
                                                message_quoted_reference['quoted_message_id']]

                                            messages_dialogue = {}
                                            messages_dialogue['DEVICE'] = message_quoted_reference['DEVICE']
                                            messages_dialogue['IMPORT_DATE'] = message_quoted_reference['IMPORT_DATE']
                                            messages_dialogue['_id'] = message_dialogue_id

                                            messages_dialogue['group'] = original_message['group']

                                            messages_dialogue['original_message'] = original_message
                                            messages_dialogue['quote_message'] = quote_message

                                            json.dump(messages_dialogue, file_messages_dialogues)
                                            file_messages_dialogues.write('\n')

                                            messages_dialogues_inserted_set.add(messages_dialogue['_id'])

                                            total_users_messages_quotes += 1

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)
        finally:
            print("Total messages:", all_total_messages_quotes, "\tTotal quote messages:", total_users_messages_quotes)


    def getPredecessors(self, graph, node, node_list):
        if (len(list(graph.predecessors(node))) == 0):
            return (node_list)
        else:
            for a_node in graph.predecessors(node):
                if(a_node is not None):
                    node_list.append(a_node)
                    node_list = self.getPredecessors(graph=graph, node=a_node, node_list=node_list)

        return(node_list)



    def getSuccessors(self, graph, node, node_list):
        if (len(list(graph.successors(node))) == 0):
            return (node_list)
        else:
            for a_node in graph.successors(node):
                if(a_node is not None):
                    node_list.append(a_node)
                    node_list = self.getSuccessors(graph=graph, node=a_node, node_list=node_list)

        return(node_list)

    def buildCascadesFiles(self, filename_messages_dialogues, path_graph_sources):
        total_cascades = 0

        import networkx as nx

        # and the following code block is not needed
        # but we want to see which module is used and
        # if and why it fails
        try:
            import pygraphviz
            from networkx.drawing.nx_agraph import write_dot
            print("using package pygraphviz")
        except ImportError:
            try:
                import pydot
                from networkx.drawing.nx_pydot import write_dot
                print("using package pydot")
            except ImportError:
                print()
                print("Both pygraphviz and pydot were not found ")
                print("see  https://networkx.github.io/documentation/latest/reference/drawing.html")
                print()
                raise



        try:
            messages_originals_publications = {}
            messages_originals_quotes = {}

            messages_originals_list_id = set()

            line_number = 0

            # Create and populate auxiliary dictionaries
            print("\tCreating and populating auxiliary dictionaries", '\tDatetime: ', datetime.now(),
                  flush=True)

            with open(filename_messages_dialogues, 'r', encoding='utf-8') as file_dialogues:
                for dialogue in file_dialogues:
                    dialogue = json.loads(dialogue)

                    line_number += 1

                    if (line_number % 100000 == 0):
                        print("\t\tDialogues processed so far", line_number, '\tDatetime: ', datetime.now(),
                              flush=True)

                    if (dialogue['group']['_id'] not in messages_originals_publications):
                        messages_originals_publications[dialogue['group']['_id']] = []

                    if(dialogue['original_message']['_id'] not in messages_originals_list_id):
                        #document = {"_id":dialogue['original_message']['_id'], "publication_date":dialogue['original_message']['publication_date']}
                        messages_originals_publications[dialogue['group']['_id']].append(dialogue['original_message'])
                        messages_originals_list_id.add(dialogue['original_message']['_id'])

                    if(dialogue['original_message']['_id'] not in messages_originals_quotes):
                        messages_originals_quotes[dialogue['original_message']['_id']] = []

                    # document = {"_id": dialogue['quote_message']['_id'],
                    #             "publication_date": dialogue['quote_message']['publication_date']}
                    messages_originals_quotes[dialogue['original_message']['_id']].append(dialogue['quote_message'])

            print("\tOrdering quote_message_lists", '\tDatetime: ', datetime.now(),
                      flush=True)
            # Order quote_message_lists
            line_number = 0
            messages_originals_quotes_copy = messages_originals_quotes
            for message_original_id, quote_messages_list in messages_originals_quotes_copy.items():
                line_number += 1

                if (line_number % 100000 == 0):
                    print("\t\tQuote messages sorted so far", line_number, "\tTotal to process: ",
                          len(messages_originals_quotes), '\tDatetime: ', datetime.now(),
                          flush=True)

                if(len(quote_messages_list) > 1):
                    result = sorted(quote_messages_list, key=itemgetter('publication_date'), reverse=True)
                    messages_originals_quotes[message_original_id] = result


            # Get oldest original_messages of each group
            print("\tProcessing cascades", '\tDatetime: ', datetime.now(),
                  flush=True)

            group_index = 0
            for group_id, values in messages_originals_publications.items():
                DG = nx.DiGraph()
                cascades_roots = {}

                group_index +=1
                print("\t>>>>>>>>> Processing group [", group_index,"]\tID:", group_id, '\tDatetime: ', datetime.now(),
                      flush=True)

                #print("Ordering a message_list of group of len", len(values), flush=True)
                messages_original_ordered = sorted(values, key=itemgetter('publication_date'), reverse=True)

                #print("Now processing ordered message_list", flush=True)

                # Process cascades of each group
                line_number = 0
                for message_original in messages_original_ordered:
                    cascades_roots[message_original['_id']] = True

                    line_number += 1

                    if (line_number % 10000 == 0):
                        print("\t\tMessages processed so far", line_number, "\tTotal to process: ",
                              len(messages_original_ordered), '\tDatetime: ', datetime.now(),
                              flush=True)

                    DG.add_node(message_original['_id'],
                                publication_date=message_original['publication_date'],
                                type = message_original['type'],
                                group_id = message_original['group']['_id'],
                                group_category=message_original['group']['category'],
                                user_id = message_original['user']['_id']
                                )

                    #print("Getting messages_quote_list", flush=True)
                    messages_quote_list = messages_originals_quotes[message_original['_id']]
                    other_line_number = 0
                    for message_quote in messages_quote_list:

                        other_line_number += 1

                        if (other_line_number % 100 == 0):
                            print("\t\t>>>>Messages quotes processed so far", other_line_number, "\tTotal to process: ",
                                  len(messages_quote_list), '\tDatetime: ', datetime.now(),
                                  flush=True)

                        if(DG.has_node(message_quote['_id']) is False):
                            DG.add_node(message_quote['_id'],
                                        publication_date=message_quote['publication_date'],
                                        type=message_quote['type'],
                                        group_id=message_quote['group']['_id'],
                                        group_category=message_quote['group']['category'],
                                        user_id=message_quote['user']['_id']
                                        )

                        DG.add_edge(message_original['_id'],message_quote['_id'])

                        if(message_quote['_id'] in cascades_roots):
                            cascades_roots[message_quote['_id']] = False

                print("\t\t>>>>Writing DOT files\tDatetime:", datetime.now(),flush=True)
                index_line = 0
                cascade_index = 1
                for message_id, is_root in cascades_roots.items():
                    index_line += 1

                    if (index_line % 1000 == 0):
                        print("\t\t>>>>Cascades processed so far", index_line, "\tTotal to process: ",
                              len(cascades_roots), '\tDatetime: ', datetime.now(),
                              flush=True)
                    if(is_root is True and DG.has_node(message_id) is True):

                        node_list = self.getSuccessors(graph=DG, node=message_id, node_list=[message_id])

                        #print(node_list)

                        H = DG.subgraph(node_list)

                        dot_filename = str(path_graph_sources+group_id+"_"+message_id+".dot")

                        write_dot(H, dot_filename)

                        total_cascades += 1
                        cascade_index +=1

                print("\t>>>>>>>>> Group [",group_id,'] had', str(cascade_index),'cascades\tDatetime: ',
                      datetime.now(),
                      flush=True)


        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)
        # finally:
        #     print("Total messages:", all_total_messages_quotes, "\tTotal quote messages:", total_users_messages_quotes)


    # Function to get the maximum width of a binary tree
    def getMaxWidth(self, graph, root, maximum_depth):
        maxWidth = 0
        h = maximum_depth

        width_by_depth_dict = {}

        # Get width of each level and compare the width with maximum width so far
        for i in range(0, h + 1):
            width = self.getWidth(graph, root, i)

            width_by_depth_dict[str(i)] = width

            if (width > maxWidth):
                maxWidth = width

        return maxWidth, width_by_depth_dict


    def getWidth(self, graph,  root, level):
        if root is None:
            return 0
        if level == 0:
            return 1
        elif level >= 1:
            neighbors = graph.successors(root)
            local_width = 0
            for neighbor in neighbors:
                local_width = self.getWidth(graph, neighbor, level - 1) + local_width
            return(local_width)

    def bfs(self,graph, start):
        queue_sizes, visited, queue = set(), set(), [start]
        while queue:
            vertex = queue.pop(0)
            if vertex not in visited:
                visited.add(vertex)
                queue.extend(list(set(list(graph[vertex])) - set(visited)))
            else:
                queue_sizes.add(len(queue))
        return queue_sizes


    def buildCascadesAttributesFiles(self, filename_cascades_static_attributes,
                                     filename_cascades_depth_oriented_attributes,
                                    filename_cascades_unique_users_over_time, path_graph_sources,
                                     filename_fake_news_messages_manual_final,filename_messages_in_cascades_mapping,
                                     filename_messages_in_cascades_mapping_tsv,filename_text_messages_info):
        try:
            text_messages_info_dict = {}
            print("\t>>>> Building dictionary of text messages info\tDatetime: ", datetime.now(), flush=True)
            #{"_id":message['_id'], "total_urls":len(urls),"total_emojis":len(emojis), "total_words":len(words),"sentiment":sentiment}
            with open(filename_text_messages_info, 'r', encoding='utf-8') as file_input:
                for text_message_info in file_input:
                    text_message_info = json.loads(text_message_info)

                    text_messages_info_dict[text_message_info['_id']] = {"sentiment":text_message_info["sentiment"],
                                                                         "total_urls":text_message_info["total_urls"],
                                                                         "total_emojis": text_message_info["total_emojis"],
                                                                         "total_words": text_message_info["total_words"]}


            messages_with_fake_news = set()
            messages_with_fake_news_dict = {}

            print("\t>>>> Building dictionary of messages falsehood verdict\tDatetime: ", datetime.now(), flush=True)
            # "document_id"  "fact_check_url" "cosine_similarity" "length_message" "length_fake_news_text"  "my_verdict_has_verdict" "cosine_similarity_str" "has_falsehood_str" "cosine_similarity_interval_str"
            with open(filename_fake_news_messages_manual_final, 'r', encoding='utf-8') as file_input:
                header = file_input.readline()
                for info in file_input:
                    info = info.replace("\n", "").split("\t")

                    if(str(info[5].replace("\"","")) == "True"):
                        messages_with_fake_news.add(info[0].replace("\"",""))
                        messages_with_fake_news_dict[info[0]] = info[1].replace("\"","")



            print("\t>>>> Creating cascades attributes files\tDatetime: ", datetime.now(), flush=True)
            total_cascades = 0


            with open(filename_cascades_static_attributes, 'w', encoding='utf-8') as file_cascades_static_attributes:
                file_cascades_static_attributes.write(str("cascade_id\tgroup_id\tgroup_category\troot_message\t"
                                                          "total_messages\ttotal_users\thas_falsehood\tmaximum_depth\t"
                                                          "unique_users\tstructural_virality\tbreadth\tfact_check_url\n"))
                with open(filename_cascades_depth_oriented_attributes, 'w',
                          encoding='utf-8') as file_cascades_depth_oriented_attributes:
                    file_cascades_depth_oriented_attributes.write(str(
                        "cascade_id\tgroup_id\tgroup_category\troot_message\ttotal_messages\ttotal_users\thas_falsehood\t"
                        "depth\ttime_passed\tbreadth\tunique_users\tfact_check_url\n"))
                    with open(filename_cascades_unique_users_over_time, 'w',
                              encoding='utf-8') as file_cascades_unique_users_over_time:
                        file_cascades_unique_users_over_time.write(str(
                            "cascade_id\tgroup_id\tgroup_category\troot_message\ttotal_messages\ttotal_users\t"
                            "has_falsehood\tunique_users\ttime_passed\tfact_check_url\n"))

                        with open(filename_messages_in_cascades_mapping_tsv, 'w',
                                  encoding='utf-8') as file_messages_in_cascades_mapping_tsv:
                            file_messages_in_cascades_mapping_tsv.write(str(
                                "cascade_id\tmessage_id\tuser_id\tgroup_id\tgroup_category\tmessage_type\troot_message\tmessage_depth\ttime_passed_since_root\t"
                                "publication_date\tcascade_has_falsehood\tmessage_has_falsehood\turl_fact_checking_news\tsentiment\ttotal_words\ttotal_emojis\t"
                                "total_urls\n"))

                            with open(filename_messages_in_cascades_mapping, 'w',
                                      encoding='utf-8') as file_messages_in_cascades_mapping:
                                for dirname, dirnames, filenames in os.walk(path_graph_sources):
                                    cascade_index = 0
                                    for graph_filename in filenames:
                                        # print("\t\t\tCascade ", graph_filename, '\tDatetime: ', datetime.now(),
                                        #       flush=True)
                                        # if (graph_filename == graph_example):
                                        try:
                                            cascade_index +=1
                                            cascade_id = str(str(graph_filename.split(".dot")[0]))

                                            total_cascades += 1

                                            if (total_cascades % 10000 == 0):
                                                print("\t\t\tCascades processed so far", total_cascades, '\tDatetime: ', datetime.now(), flush=True)

                                            DG = nx.DiGraph(nx.nx_pydot.read_dot(str(str(dirname)+str(graph_filename))))

                                            # (1) >>>>>>>>>> Depth
                                            first_node = list(nx.topological_sort(DG))[0]
                                            depths = nx.shortest_path_length(DG, first_node)
                                            maximum_depth = sorted(depths.items(), key=operator.itemgetter(1), reverse=True)[0][1]

                                            # Getting cascade credentials
                                            group_id = DG.node[first_node]['group_id']
                                            root_message = first_node
                                            group_category = DG.node[first_node]['group_category']

                                            # (2) >>>>>>>>>> Structural Virality
                                            G = DG.to_undirected()
                                            structural_virality = nx.average_shortest_path_length(G)

                                            first_cascade_message_datetime = self.getDateFormatted(
                                                DG.node[first_node]['publication_date'])

                                            # (3) >>>>>>>>>> Size/Unique Users
                                            users_in_cascade = set()
                                            graph_nodes = DG.nodes()
                                            for a_node in graph_nodes:
                                                users_in_cascade.add(DG.node[a_node]['user_id'])

                                            unique_users = len(users_in_cascade)

                                            # Check if cascade has falsehood
                                            has_falsehood = not set(messages_with_fake_news).isdisjoint(graph_nodes)

                                            # Get fact checking URL
                                            fact_check_url = ""

                                            if(has_falsehood):
                                                for a_node in graph_nodes:
                                                    if(a_node in messages_with_fake_news_dict):
                                                        fact_check_url = messages_with_fake_news_dict[a_node]
                                                        break


                                            total_cascade_messages = len(graph_nodes)
                                            total_cascade_users = unique_users

                                            # @@@@@@@@@@@@@@@ Unique users over time
                                            graph_nodes_publication_date_dict = {}
                                            for a_node in graph_nodes:
                                                if(DG.node[a_node]['publication_date'] not in graph_nodes_publication_date_dict):
                                                    graph_nodes_publication_date_dict[DG.node[a_node]['publication_date']] = []

                                                graph_nodes_publication_date_dict[DG.node[a_node]['publication_date']].append(DG.node[a_node]['user_id'])

                                            messages_sorted_by_publication_date = OrderedDict(sorted(graph_nodes_publication_date_dict.items()))
                                            unique_users_over_time_list = []
                                            for message_datetime_info, user_id_list in messages_sorted_by_publication_date.items():
                                                for user_id in user_id_list:
                                                    if (user_id not in unique_users_over_time_list):
                                                        unique_users_over_time_list.append(user_id)

                                                        difference = self.getDateFormatted(message_datetime_info) - first_cascade_message_datetime
                                                        minutes = difference.total_seconds() / 60

                                                        # ######## WRITING UNIQUE USERS OVER TIME
                                                        file_cascades_unique_users_over_time.write(str(
                                                            str(cascade_id) + "\t" + str(group_id) + "\t" + str(
                                                                group_category) + "\t" + str(root_message) +
                                                            "\t" + str(total_cascade_messages) + "\t" + str(
                                                                total_cascade_users) + "\t" + str(has_falsehood) + "\t" +
                                                            str(len(unique_users_over_time_list)) + "\t" + str(minutes) + "\t" +
                                                            str(fact_check_url)+"\n"))


                                            # (4) >>>>>>>>>> Breadth
                                            max_breadth, breadth_by_depth_dict = self.getMaxWidth(graph=DG, root=first_node, maximum_depth=maximum_depth)

                                            # ######## WRITING STATIC FEATURES
                                            file_cascades_static_attributes.write(str(
                                                str(cascade_id)+"\t"+str(group_id)+"\t"+str(group_category)+"\t"+str(root_message)+
                                                "\t"+str(total_cascade_messages)+"\t"+str(total_cascade_users)+"\t"+str(has_falsehood)+"\t"+
                                                str(maximum_depth)+"\t"+str(unique_users)+"\t"+
                                                str(structural_virality)+"\t"+str(max_breadth)+ "\t" +
                                                            str(fact_check_url)+"\n"))


                                            # *********** DEPTH ORIENTED FEATURES
                                            # depths_info = sorted(depths.items(), key=operator.itemgetter(1), reverse=True)
                                            depths_info = sorted(depths.items(), key=operator.itemgetter(1))
                                            depth_datetimes = {}
                                            unique_users_by_depth_dict = {}
                                            for a_node, depth in depths_info:
                                                # if(int(depth) >= 0 and int(depth) <= 4):
                                                #     a = 10

                                                if(str(depth) not in depth_datetimes):
                                                    depth_datetimes[str(depth)] = []

                                                if (str(depth) not in unique_users_by_depth_dict):
                                                    unique_users_by_depth_dict[str(depth)] = set()

                                                depth_datetimes[str(depth)].append(self.getDateFormatted(DG.node[a_node]['publication_date']))

                                                unique_users_by_depth_dict[str(depth)].add(DG.node[a_node]['user_id'])

                                            for depth, publication_date_list in depth_datetimes.items():
                                                first_message_at_depth = sorted(publication_date_list, reverse=False)[0]

                                                difference = first_message_at_depth - first_cascade_message_datetime
                                                minutes = difference.total_seconds() / 60

                                                unique_users_at_depth = len(unique_users_by_depth_dict[depth])
                                                breadth_at_depth = breadth_by_depth_dict[depth]

                                                # ######## WRITING DEPTH ORIENTED FEATURES
                                                file_cascades_depth_oriented_attributes.write(str(
                                                    str(cascade_id) + "\t" + str(group_id) + "\t" + str(
                                                        group_category) + "\t" + str(root_message) +
                                                    "\t" + str(total_cascade_messages) + "\t" + str(
                                                        total_cascade_users) + "\t" + str(has_falsehood) + "\t" +
                                                    str(depth) + "\t" + str(minutes) + "\t" +
                                                    str(breadth_at_depth) + "\t" + str(unique_users_at_depth) +"\t"+
                                                            fact_check_url+"\n"))

                                            # ######## WRITING MAPPING FILE
                                            for a_node in graph_nodes:
                                                message_id = a_node
                                                user_id = DG.node[a_node]['user_id'].replace("\"", "")
                                                group_id = DG.node[a_node]['group_id'].replace("\"", "")
                                                group_category = DG.node[a_node]['group_category'].replace("\"", "")
                                                publication_date = DG.node[a_node]['publication_date'].replace("\"",
                                                                                                               "")
                                                message_type = DG.node[a_node]['type'].replace("\"", "")

                                                message_depth = 0
                                                for another_node, depth in depths_info:
                                                    if(a_node == another_node):
                                                        message_depth = str(depth)
                                                        break

                                                time_passed_since_root = 0
                                                for depth, publication_date_list in depth_datetimes.items():
                                                    if(depth == message_depth):
                                                        first_message_at_depth = \
                                                        sorted(publication_date_list, reverse=False)[0]

                                                        difference = first_message_at_depth - first_cascade_message_datetime
                                                        minutes = difference.total_seconds() / 60

                                                        time_passed_since_root = minutes
                                                        break

                                                document = {}
                                                document["_id"] = message_id
                                                document["cascade_id"] = cascade_id
                                                document["message_id"] = message_id
                                                document["user_id"] = user_id
                                                document["group_id"] =  group_id
                                                document["group_category"] = group_category
                                                document["message_type"] = message_type
                                                document["root_message"] =  (first_node == a_node)
                                                document["message_depth"] = message_depth
                                                document["time_passed_since_root"] = time_passed_since_root
                                                document["publication_date"] = publication_date
                                                document['cascade_has_falsehood'] = has_falsehood
                                                document['message_has_falsehood'] = (a_node in messages_with_fake_news)
                                                document["url_fact_checking_news"] = fact_check_url

                                                sentiment = None
                                                total_words = None
                                                total_emojis = None
                                                total_urls = None

                                                if(message_id in text_messages_info_dict):
                                                    sentiment = text_messages_info_dict[message_id]["sentiment"]
                                                    total_words = text_messages_info_dict[message_id]["total_words"]
                                                    total_emojis = text_messages_info_dict[message_id]["total_emojis"]
                                                    total_urls = text_messages_info_dict[message_id]["total_urls"]

                                                document["sentiment"] = sentiment
                                                document["total_words"] = total_words
                                                document["total_emojis"] = total_emojis
                                                document["total_urls"] = total_urls

                                                json.dump(document, file_messages_in_cascades_mapping)
                                                file_messages_in_cascades_mapping.write("\n")

                                                file_messages_in_cascades_mapping_tsv.write(str(
                                                    cascade_id+"\t"+message_id+"\t"+user_id+"\t"+group_id+"\t"+group_category+"\t"+
                                                    message_type+"\t"+str((first_node == a_node))+"\t"+str(message_depth)+"\t"+str(time_passed_since_root)+"\t"+
                                                    str(publication_date)+"\t"+str(has_falsehood)+"\t"+str((a_node in messages_with_fake_news))+"\t"+
                                                    str(fact_check_url)+"\t"+str(sentiment)+"\t"+str(total_words)+"\t"+str(total_emojis)+"\t"+
                                                    str(total_urls)+"\n"))

                                        except Exception as e:
                                            exc_type, exc_obj, exc_tb = sys.exc_info()
                                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno,
                                                  '\tDatetime: ', datetime.now(),
                                                  flush=True)
                                            print("ERROR PROCESSING CASCADE FILE:",str(str(dirname)+str(graph_filename)), flush=True)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)


    def getIncomingMotifTemplateListByNodeTotal(self, total_nodes):
        motif_incoming_star_edge_list = []
        for i in range(2, total_nodes + 1):
            motif_incoming_star_edge_list.extend([(i, 1)])

        return (motif_incoming_star_edge_list)

    def getOutgoingMotifTemplateListByNodeTotal(self, total_nodes):
        motif_outgoing_star_edge_list = []
        for i in range(2, total_nodes + 1):
            motif_outgoing_star_edge_list.extend([(1, i)])

        return (motif_outgoing_star_edge_list)

    def getChainMotifTemplateListByNodeTotal(self, total_nodes):
        motif_chain_edge_list = []
        for i in range(1, total_nodes + 1):
            motif_chain_edge_list.extend([(i, (i + 1))])

        return (motif_chain_edge_list)

    def getLoopMotifTemplateListByNodeTotal(self, total_nodes):
        motif_loop_edge_list = []
        initial_node = 1
        for i in range(1, total_nodes):
            motif_loop_edge_list.extend([(i, (i + 1))])

        motif_loop_edge_list.extend([(total_nodes, (initial_node))])

        return (motif_loop_edge_list)


    def buildUserRelationCascadeMotifsFile(self, filename_cascades_static_attributes,
                                            path_graph_sources, filename_user_relations_motifs,
                                            is_user_cascades_by_cascades):
        try:
            cascade_info_dict = {}

            with open(filename_cascades_static_attributes, 'r', encoding='utf-8') as file_cascades_static_attributes:
                #cascade_id group_id group_category root_message total_messages total_users has_falsehood maximum_depth unique_users structural_virality breadth fact_check_url
                header = file_cascades_static_attributes.readline()
                for cascade_info_line in file_cascades_static_attributes:
                    cascade_info = cascade_info_line.replace("\n", "").split("\t")
                    if(cascade_info[0] not in cascade_info_dict):
                        cascade_info_dict[cascade_info[0]] = {"group_id":cascade_info[1], "group_category":cascade_info[2],
                                                              "root_message": cascade_info[3],
                                                              "total_messages": cascade_info[4],
                                                              "total_users":cascade_info[5], "has_falsehood":cascade_info[6]}



            total_cascades = 0

            motif_dyadic_edge_list = [(1, 2), (2, 1)]
            motif_self_loop_edge_list = [(1, 1)]

            with open(filename_user_relations_motifs, 'w', encoding='utf-8') as file_user_relations_motifs:
                if(is_user_cascades_by_cascades is True):
                    file_user_relations_motifs.write(str("cascade_id\tgroup_id\tgroup_category\troot_message\t"
                                                              "total_messages\ttotal_users\tcascade_has_falsehood\tmotif_name\ttotal_nodes\ttotal_edges\ttotal_edge_weight_on_motifs\ttotal_motifs\n"))
                else:
                    file_user_relations_motifs.write(str("group_id\tgroup_category\tmotif_name\ttotal_nodes\ttotal_edges\ttotal_edge_weight_on_motifs\ttotal_motifs\n"))

                for dirname, dirnames, filenames in os.walk(path_graph_sources):
                    cascade_index = 0
                    for graph_filename in filenames:
                        try:
                            cascade_index +=1
                            cascade_id = str(str(graph_filename.split(".dot")[0]))

                            total_cascades += 1

                            if (total_cascades % 1000 == 0):
                                print("\t\t\tCascades processed so far", total_cascades, '\tDatetime: ', datetime.now(), flush=True)

                            DG = nx.DiGraph(nx.nx_pydot.read_dot(str(str(dirname)+str(graph_filename))))

                            motif_totals_dict = {"dyadic": {}, "self_loop": {}, "chain":{}, "loop":{},
                                                 "incoming_star":{}, "outgoing_star":{}}

                            V = DG.nodes()

                            # ------ DYADIC
                            motif_dyadic = nx.DiGraph(motif_dyadic_edge_list)
                            motif_name = "dyadic"
                            total_nodes = 2
                            motif_totals_dict[motif_name][str(total_nodes)] = {"total_edges":0, "total_edge_weight_on_motifs":0, "total_motifs":0}

                            for subV in itertools.combinations(V, total_nodes):
                                subG = nx.subgraph(DG, subV)
                                if nx.is_isomorphic(subG, motif_dyadic):
                                    motif_totals_dict[motif_name][str(total_nodes)]['total_motifs']+=1

                                    for node, edge_info in subG.adj.items():
                                        for node_to, weight in edge_info.items():
                                            #print(node_to, weight, flush=True)
                                            motif_totals_dict[motif_name][str(total_nodes)]['total_edges'] += 1
                                            motif_totals_dict[motif_name][str(total_nodes)]['total_edge_weight_on_motifs'] += int(weight['weight'])

                            # ------ SELF-LOOP
                            motif_self_loop = nx.DiGraph(motif_self_loop_edge_list)
                            motif_name = "self_loop"
                            total_nodes = 1
                            motif_totals_dict[motif_name][str(total_nodes)] = {"total_edges": 0,
                                                                               "total_edge_weight_on_motifs": 0,
                                                                               "total_motifs": 0}

                            for subV in itertools.combinations(V, total_nodes):
                                subG = nx.subgraph(DG, subV)
                                if nx.is_isomorphic(subG, motif_self_loop):
                                    motif_totals_dict[motif_name][str(total_nodes)]['total_motifs'] += 1

                                    for node, edge_info in subG.adj.items():
                                        for node_to, weight in edge_info.items():
                                            motif_totals_dict[motif_name][str(total_nodes)][
                                                'total_edges'] += 1
                                            motif_totals_dict[motif_name][str(total_nodes)][
                                                'total_edge_weight_on_motifs'] += int(weight['weight'])


                            for i in range(1,len(DG.nodes())):
                                motif_chain_edge_list = self.getChainMotifTemplateListByNodeTotal(total_nodes=i)

                                # ------ CHAIN
                                motif_chain = nx.DiGraph(motif_chain_edge_list)
                                motif_name = "chain"
                                total_nodes = i+1
                                motif_totals_dict[motif_name][str(total_nodes)] = {"total_edges": 0,
                                                                                   "total_edge_weight_on_motifs": 0,
                                                                                   "total_motifs": 0}

                                for subV in itertools.combinations(V, total_nodes):
                                    subG = nx.subgraph(DG, subV)
                                    if nx.is_isomorphic(subG, motif_chain):
                                        motif_totals_dict[motif_name][str(total_nodes)]['total_motifs'] += 1

                                        for node, edge_info in subG.adj.items():
                                            for node_to, weight in edge_info.items():
                                                motif_totals_dict[motif_name][str(total_nodes)]['total_edges'] += 1
                                                motif_totals_dict[motif_name][str(total_nodes)][
                                                    'total_edge_weight_on_motifs'] += int(weight['weight'])


                                if(i >= 3):
                                    motif_loop_edge_list = self.getLoopMotifTemplateListByNodeTotal(total_nodes=i)
                                    # ------ LOOP
                                    motif_loop = nx.DiGraph(motif_loop_edge_list)
                                    motif_name = "loop"
                                    total_nodes = i
                                    motif_totals_dict[motif_name][str(total_nodes)] = {"total_edges": 0,
                                                                                       "total_edge_weight_on_motifs": 0,
                                                                                       "total_motifs": 0}

                                    for subV in itertools.combinations(V, total_nodes):
                                        subG = nx.subgraph(DG, subV)
                                        if nx.is_isomorphic(subG, motif_loop):
                                            motif_totals_dict[motif_name][str(total_nodes)]['total_motifs'] += 1

                                            for node, edge_info in subG.adj.items():
                                                for node_to, weight in edge_info.items():
                                                    motif_totals_dict[motif_name][str(total_nodes)]['total_edges'] += 1
                                                    motif_totals_dict[motif_name][str(total_nodes)][
                                                        'total_edge_weight_on_motifs'] += int(weight['weight'])

                                    motif_incoming_star_edge_list = self.getIncomingMotifTemplateListByNodeTotal(
                                        total_nodes=i)
                                    motif_outgoing_star_edge_list = self.getOutgoingMotifTemplateListByNodeTotal(
                                        total_nodes=i)

                                    # ------ INCOMING STAR
                                    motif_incoming_star = nx.DiGraph(motif_incoming_star_edge_list)
                                    motif_name = "incoming_star"
                                    total_nodes = i
                                    motif_totals_dict[motif_name][str(total_nodes)] = {"total_edges": 0,
                                                                                       "total_edge_weight_on_motifs": 0,
                                                                                       "total_motifs": 0}

                                    for subV in itertools.combinations(V, total_nodes):
                                        subG = nx.subgraph(DG, subV)
                                        if nx.is_isomorphic(subG, motif_incoming_star):
                                            motif_totals_dict[motif_name][str(total_nodes)]['total_motifs'] += 1

                                            for node, edge_info in subG.adj.items():
                                                for node_to, weight in edge_info.items():
                                                    motif_totals_dict[motif_name][str(total_nodes)]['total_edges'] += 1
                                                    motif_totals_dict[motif_name][str(total_nodes)][
                                                        'total_edge_weight_on_motifs'] += int(weight['weight'])

                                    # ------ OUTGOING STAR
                                    motif_outcoming_star = nx.DiGraph(motif_outgoing_star_edge_list)
                                    motif_name = "outgoing_star"
                                    total_nodes = i
                                    motif_totals_dict[motif_name][str(total_nodes)] = {"total_edges": 0,
                                                                                       "total_edge_weight_on_motifs": 0,
                                                                                       "total_motifs": 0}

                                    for subV in itertools.combinations(V, total_nodes):
                                        subG = nx.subgraph(DG, subV)
                                        if nx.is_isomorphic(subG, motif_outcoming_star):
                                            motif_totals_dict[motif_name][str(total_nodes)]['total_motifs'] += 1

                                            for node, edge_info in subG.adj.items():
                                                for node_to, weight in edge_info.items():
                                                    motif_totals_dict[motif_name][str(total_nodes)]['total_edges'] += 1
                                                    motif_totals_dict[motif_name][str(total_nodes)][
                                                        'total_edge_weight_on_motifs'] += int(weight['weight'])

                            #print('Writing motif file\tDatetime: ', datetime.now(), flush=True)
                            for motif_name, motif_nodes_info in motif_totals_dict.items():
                                for motif_nodes, motif_info in motif_nodes_info.items():
                                    if motif_info['total_motifs'] > 0:
                                        group_id = cascade_info_dict[cascade_id]["group_id"]
                                        group_category = cascade_info_dict[cascade_id]["group_category"]
                                        if(is_user_cascades_by_cascades is True):
                                            root_message = cascade_info_dict[cascade_id]["root_message"]
                                            total_cascade_messages = cascade_info_dict[cascade_id]["total_messages"]
                                            total_cascade_users = cascade_info_dict[cascade_id]["total_users"]
                                            has_falsehood = cascade_info_dict[cascade_id]["has_falsehood"]

                                            file_user_relations_motifs.write(str(
                                                str(cascade_id) + "\t" + str(group_id) + "\t" + str(group_category) + "\t" + str(
                                                    root_message) +
                                                "\t" + str(total_cascade_messages) + "\t" + str(total_cascade_users) + "\t" + str(
                                                    has_falsehood) + "\t" +
                                                str(motif_name) + "\t" + str(motif_nodes) + "\t" + str(motif_info['total_edges']) + "\t" +
                                                str(motif_info['total_edge_weight_on_motifs'])+ "\t" + str(motif_info['total_motifs']) + "\n"))
                                        else:
                                            file_user_relations_motifs.write(str(str(group_id) + "\t" + str(
                                                    group_category) + "\t" +
                                                str(motif_name) + "\t" + str(motif_nodes) + "\t" + str(
                                                    motif_info['total_edges']) + "\t" +
                                                str(motif_info['total_edge_weight_on_motifs']) + "\t" + str(
                                                    motif_info['total_motifs']) + "\n"))

                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno,
                                  '\tDatetime: ', datetime.now(),
                                  flush=True)
                            print("ERROR PROCESSING CASCADE FILE:",str(str(dirname)+str(graph_filename)), flush=True)
                            #sys.exit(1)

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ', datetime.now(),
                  flush=True)


    def buildSimilarTextsFromFactCheckingSitesFile(self, filename_clean_verifiable_texts,
                                                                 filename_clean_fake_news_texts_from_fact_checking,
                                                                 filename_similar_texts_with_fact_checking_texts_primary,
                                                   filename_text_from_url_messages_dialogues):
        try:
            from fuzzywuzzy import fuzz
            from fuzzywuzzy import process

            message_id_url_dict = {}
            with open(filename_text_from_url_messages_dialogues, 'r',
                      encoding='utf-8') as file_text_from_url_messages_dialogues:
                for message in file_text_from_url_messages_dialogues:
                    try:
                        message = json.loads(message)

                        message_id_url_dict[message['_id']] = message["message_id"]
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print('\nError (DID NOT STOP): ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ',
                              datetime.now(),
                              flush=True)


            from itertools import combinations
            # Get all possible variations from fake news texts
            fake_news_texts_list_dict = {}
            with open(filename_clean_fake_news_texts_from_fact_checking, 'r',
                      encoding='utf-8') as file_clean_fake_news_texts_from_fact_checking:
                for message in file_clean_fake_news_texts_from_fact_checking:
                    try:
                        message = json.loads(message)

                        fact_check_url = message["url"].split("_")[len(message["url"].split("_")) - 2]

                        if(fact_check_url not in fake_news_texts_list_dict):
                            fake_news_texts_list_dict[fact_check_url] = []

                        fake_news_texts_list_dict[fact_check_url].append(message['text'])
                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        print('\nError (DID NOT STOP) on message:', message, '\tThe error:', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ',
                              datetime.now(),
                              flush=True)

            fake_news_texts_dict = {}
            real_total_fake_news_texts = 0
            for url, text_list in fake_news_texts_list_dict.items():
                all_forward_combinations = [' '.join(text_list[i:j]) for i, j in combinations(range(len(text_list) + 1), 2)]
                fake_news_texts_dict[url] = []
                for text in all_forward_combinations:
                    fake_news_texts_dict[url].append(text)
                    real_total_fake_news_texts +=1

            WORD = re.compile(r'\w+')

            def get_cosine(vec1, vec2):
                intersection = set(vec1.keys()) & set(vec2.keys())
                numerator = sum([vec1[x] * vec2[x] for x in intersection])

                sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
                sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
                denominator = math.sqrt(sum1) * math.sqrt(sum2)

                if not denominator:
                    return 0.0
                else:
                    return float(numerator) / denominator

            def text_to_vector(text):
                words = WORD.findall(text)
                return Counter(words)

            messages_dict = {}
            total_documents_without_message_id = 0
            real_total_verifiable_texts = 0
            with open(filename_clean_verifiable_texts, 'r',
                      encoding='utf-8') as file_clean_verifiable_texts:
                for message in file_clean_verifiable_texts:
                    message = json.loads(message)

                    message_id = message['message_id']

                    if (message_id == "" and message["source_text_type"] == "URL" and message['url'] in message_id_url_dict):
                        message_id = message_id_url_dict[message['url']]

                    if(message_id != ""):
                        if(message_id not in messages_dict):
                            messages_dict[message_id] = []
                        messages_dict[message_id].append(message['text'])
                        real_total_verifiable_texts +=1
                    else:
                        total_documents_without_message_id +=1

            print("Total URL document without message_id:",total_documents_without_message_id, flush=True)
            print("$$$$$$$$$$$$ Checking", real_total_verifiable_texts ,"messages/URLs from dialogues in",real_total_fake_news_texts,"fake news texts.\tDatetime: ",
                  datetime.now(),
                  flush=True)
            line_number = 0
            total_found = 0
            with open(filename_similar_texts_with_fact_checking_texts_primary, 'w',
                      encoding='utf-8') as file_similar_texts_with_fact_checking_texts_primary:
                file_similar_texts_with_fact_checking_texts_primary.write(str("document_id\tfact_check_url\tcosine_similarity\tlength_message\tlength_fake_news_text\n"))
                for message_id, message_text_list in messages_dict.items():
                    line_number += 1

                    if(line_number % 1000 == 0):
                        print("\t>>> Processing message index", line_number, '\tDatetime: ',
                              datetime.now(),
                              flush=True)

                    found_similar_text = False
                    for message_text  in message_text_list:
                        if (found_similar_text is False):
                            vector2 = text_to_vector(message_text)

                            if (len(vector2) >= 5):
                                for fake_news_url, fake_news_text_list in fake_news_texts_dict.items():
                                    if(found_similar_text is False):
                                        for fake_news_text in fake_news_text_list:

                                            vector1 = text_to_vector(fake_news_text)

                                            if (len(vector1) >= 5):
                                                cosine_similarity = get_cosine(vector1, vector2)
                                                if (cosine_similarity >= 0.5):
                                                    file_similar_texts_with_fact_checking_texts_primary.write(
                                                        str(str(message_id) +
                                                            "\t" + str(fake_news_url) + "\t" + str(
                                                            cosine_similarity) + "\t" + str(len(vector2)) + "\t" + str(
                                                            len(vector1)) + "\n"))
                                                    total_found += 1
                                                    print("\t\t>>> Writing similar text", cosine_similarity,
                                                          "\tTotal found so far: ", total_found,
                                                          "\tProcessing text index:", line_number, '\tDatetime: ',
                                                          datetime.now(), flush=True)
                                                    if (cosine_similarity >= 0.9):
                                                        found_similar_text = True
                                                        print(
                                                            "\t\t\t>>> Found similarity >=0.9... stopping iteration\tDatetime: ",
                                                            datetime.now(), flush=True)
                                                        break
                                                levenshtein_distance = fuzz.ratio(message_text, fake_news_text)

                                                if (levenshtein_distance >= 50):
                                                    file_similar_texts_with_fact_checking_texts_primary.write(str(str(message_id)+
                                                        "\t"+str(fake_news_url)+"\t"+str(levenshtein_distance)+"\t"+str(len(vector2))+"\t"+str(len(vector1))+"\n"))
                                                    total_found +=1
                                                    print("\t\t>>> Writing similar text", levenshtein_distance, "\tTotal found so far: ", total_found, "\tProcessing text index:", line_number , '\tDatetime: ',datetime.now(),flush=True)
                                                    if(levenshtein_distance >= 90):
                                                        found_similar_text = True
                                                        print("\t\t\t>>> Found similarity >=0.9... stopping iteration\tDatetime: ",datetime.now(), flush=True)
                                                        break
                                    else:
                                        break
                        else:
                            break

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ',
                  datetime.now(),
                  flush=True)


    def lemmatize_text_portuguese(self, nlp_pt, text):
        return " ".join(token.lemma_ for token in nlp_pt(text))

    def getWordsLematizedWithoutStopWords(self, text, nlp_pt, stopwords_list=None):
        text = self.lemmatize_text_portuguese(text=text, nlp_pt=nlp_pt)
        text = re.sub(r"https?://\S+", "", text)
        text = re.sub(r"\b\d+\b", "", text)
        words = [word.lower() for word in re.findall("\w+", text)]
        words = [word for word in words if word not in stopwords_list] if stopwords_list else words
        words = [word for word in words if len(word) > 2]
        return words

    def getURLs(self, text):
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+',
                          text)
        return urls

    def buildCleanTextFromMessagesOnCascadesFile(self, filename_portuguese_stopwords, filename_messages_dialogues, filename_text_from_url_messages_dialogues,
                                                 filename_fake_news_texts_from_fact_checking, filename_clean_verifiable_texts,
                                                 filename_clean_fake_news_texts_from_fact_checking):
        try:
            # Lemmatizers
            import spacy
            nlp_pt = spacy.load("pt")

            from langid.langid import LanguageIdentifier, model
            identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)

            messages_processed_ids = set()

            messages_urls_dict = {}

            line_number = 0

            # Stopwords
            stopwords_portuguese = set([line.rstrip() for line in open(filename_portuguese_stopwords, encoding="utf-8")])

            print("\t>>>> Building clean verifiable texts file\tDatetime: ", datetime.now(), flush=True)
            with open(filename_clean_verifiable_texts, 'w', encoding='utf-8') as file_clean_verifiable_texts:
                with open(filename_messages_dialogues, 'r', encoding='utf-8') as file_messages_dialogues:
                    for message_dialogue in file_messages_dialogues:
                        message_dialogue = json.loads(message_dialogue)

                        line_number += 1
                        if (line_number % 10000 == 0):
                            print("\t>>> Processing dialogue index", line_number, '\tDatetime: ',
                                  datetime.now(),
                                  flush=True)


                        for message_source in ['original_message', 'quote_message']:
                            if message_dialogue[message_source]['_id'] not in messages_processed_ids and message_dialogue[message_source]['type'] == 'text':
                                new_text = message_dialogue[message_source]['text'].lower()
                                my_val = str(new_text).rstrip("\n").rstrip("\t").rstrip("\r")
                                my_val = str(my_val).replace('\r', " ").replace('\n', " ").replace('\t', " ")
                                new_text = my_val.strip()

                                messages_urls_dict[message_dialogue[message_source]['_id']] = self.getURLs(text=new_text)

                                langid_language_detected_result = identifier.classify(new_text)
                                langid_language_detected = langid_language_detected_result[0]
                                language_probability = langid_language_detected_result[1]


                                if(langid_language_detected == 'pt' and float(language_probability) >= 0.9):

                                    text = new_text
                                    words = self.getWordsLematizedWithoutStopWords(text=text, stopwords_list=stopwords_portuguese, nlp_pt = nlp_pt)
                                    #words = self.get_words(text, stopwords_portuguese, None)

                                    fullStr = ' '.join(words)

                                    document = {"message_id":message_dialogue[message_source]['_id'], "text":fullStr, "source_text_type":"MESSAGE", "url":""}
                                    json.dump(document, file_clean_verifiable_texts)
                                    file_clean_verifiable_texts.write("\n")

                with open(filename_text_from_url_messages_dialogues, 'r', encoding='utf-8') as file_text_from_url_messages_dialogues:
                    for text_url_info in file_text_from_url_messages_dialogues:
                        try:
                            text_url_info = json.loads(text_url_info)

                            new_text = text_url_info['text'].lower()
                            my_val = str(new_text).rstrip("\n").rstrip("\t").rstrip("\r")
                            my_val = str(my_val).replace('\r', " ").replace('\n', " ").replace('\t', " ")
                            new_text = my_val.strip()

                            text = new_text
                            words = self.getWordsLematizedWithoutStopWords(text=text, stopwords_list=stopwords_portuguese,
                                                                           nlp_pt=nlp_pt)
                            # words = self.get_words(text, stopwords_portuguese, None)

                            fullStr = ' '.join(words)

                            my_message_id = ""
                            for message_id, url_list in messages_urls_dict.items():
                                for url in url_list:
                                    if (url == text_url_info['_id']):
                                        my_message_id = message_id
                                        break

                            document = {"message_id": my_message_id, "text": fullStr, "source_text_type": "URL", "url": text_url_info['_id']}
                            json.dump(document, file_clean_verifiable_texts)
                            file_clean_verifiable_texts.write("\n")
                        except Exception as e:
                            exc_type, exc_obj, exc_tb = sys.exc_info()
                            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                            print('\nError DID NOT STOP: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno,
                                  '\tDatetime: ',
                                  datetime.now(),
                                  flush=True)

            print("\t>>>> Building clean texts from fact checking sites file\tDatetime: ", datetime.now(), flush=True)
            with open(filename_clean_fake_news_texts_from_fact_checking, 'w', encoding='utf-8') as file_clean_fake_news_texts_from_fact_checking:
                with open(filename_fake_news_texts_from_fact_checking, 'r', encoding='utf-8') as file_fake_news_texts_from_fact_checking:
                    all_documents = json.load(file_fake_news_texts_from_fact_checking)
                    for text_url_info in all_documents:
                        new_text = text_url_info['text'].lower()
                        my_val = str(new_text).rstrip("\n").rstrip("\t").rstrip("\r")
                        my_val = str(my_val).replace('\r', " ").replace('\n', " ").replace('\t', " ")
                        new_text = my_val.strip()

                        text = new_text
                        words = self.getWordsLematizedWithoutStopWords(text=text, stopwords_list=stopwords_portuguese,
                                                                       nlp_pt=nlp_pt)
                        # words = self.get_words(text, stopwords_portuguese, None)

                        fullStr = ' '.join(words)

                        document = {"text": fullStr, "url": text_url_info['_id']}
                        json.dump(document, file_clean_fake_news_texts_from_fact_checking)
                        file_clean_fake_news_texts_from_fact_checking.write("\n")


        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print('\nError: ', e, '\tDetails: ', exc_type, fname, exc_tb.tb_lineno, '\tDatetime: ',
                  datetime.now(),
                  flush=True)


def main():
    try:
        start_time = datetime.now()

        attentionCascades = AttentionCascades()

        DATA_DIRECTORY = "/home/<<user>>/data/"

        path_graph_sources = str(DATA_DIRECTORY+"attention_cascades/cascades/dot/")
        filename_messages_dialogues=str(DATA_DIRECTORY+"attention_cascades/dialogues.jsonl")
        filename_messages =str(DATA_DIRECTORY+"attention_cascades/messages.jsonl")
        filename_quote_messages = str(DATA_DIRECTORY+"attention_cascades/quote_messages.jsonl")
        filename_messages_quotes_references =str(DATA_DIRECTORY+"attention_cascades/messages_quotes_references.jsonl")
        filename_cascades_static_attributes = str(DATA_DIRECTORY+"attention_cascades/cascades_static_attributes.tsv")
        filename_cascades_depth_oriented_attributes = str(DATA_DIRECTORY+"attention_cascades/cascades_depth_oriented_attributes.tsv")
        filename_cascades_unique_users_over_time = str(DATA_DIRECTORY+"attention_cascades/cascades_unique_users_over_time_attributes.tsv")
        filename_messages_in_cascades_mapping =  str(DATA_DIRECTORY+"attention_cascades/messages_in_cascades_mapping.jsonl")
        filename_messages_in_cascades_mapping_tsv = str(DATA_DIRECTORY+"attention_cascades/messages_in_cascades_mapping.tsv")
        filename_text_messages_info= str(DATA_DIRECTORY+"attention_cascades/text_messages_info.jsonl")

        path_user_relations_individual_graph_sources = str(DATA_DIRECTORY+"attention_cascades/user_relations_by_cascade_cascades/dot/")
        filename_clean_verifiable_texts = str(DATA_DIRECTORY+"attention_cascades/clean_verifiable_texts_from_cascades.jsonl")
        filename_clean_fake_news_texts_from_fact_checking = str(DATA_DIRECTORY+"attention_cascades/clean_texts_from_urls_from_fact_checking_sites.json")
        filename_similar_texts_with_fact_checking_texts_primary = str(DATA_DIRECTORY+"attention_cascades/similar_texts_with_fact_checking_texts_primary.tsv")
        filename_text_from_url_messages_dialogues = str(DATA_DIRECTORY+"attention_cascades/dialogues_text_from_url_messages_new.jsonl")
        filename_fake_news_texts_from_fact_checking  = str(DATA_DIRECTORY+"attention_cascades/texts_from_urls_from_fact_checking_sites.json")
        filename_portuguese_stopwords = str(DATA_DIRECTORY+"attention_cascades/stopwords-portuguese.txt")

        print(">>>>>>>> Running attentionCascades.buildDialoguesCollection method", flush=True)
        attentionCascades.buildDialoguesCollection(filename_messages_dialogues=filename_messages_dialogues,
                                               filename_messages=filename_messages,
                                               filename_quote_messages=filename_quote_messages,
                                               filename_messages_quotes_references=filename_messages_quotes_references)

        print(">>>>>>>> Running attentionCascades.buildCascadesFiles method", flush=True)
        attentionCascades.buildCascadesFiles(filename_messages_dialogues=filename_messages_dialogues,
                                         path_graph_sources=path_graph_sources)

        print(">>>>>>>> Running attentionCascades.buildCleanTextFromMessagesOnCascadesFile method", flush=True)
        attentionCascades.buildCleanTextFromMessagesOnCascadesFile(filename_portuguese_stopwords=filename_portuguese_stopwords, 
                                                               filename_messages_dialogues=filename_messages_dialogues,
                                                 filename_text_from_url_messages_dialogues=filename_text_from_url_messages_dialogues,
                                                 filename_fake_news_texts_from_fact_checking=filename_fake_news_texts_from_fact_checking,
                                                 filename_clean_verifiable_texts=filename_clean_verifiable_texts,
                                                 filename_clean_fake_news_texts_from_fact_checking=filename_clean_fake_news_texts_from_fact_checking)

        print(">>>>>>>> Running attentionCascades.buildCascadesAttributesFiles method", flush=True)
        attentionCascades.buildSimilarTextsFromFactCheckingSitesFile(filename_clean_verifiable_texts=filename_clean_verifiable_texts,
                                                   filename_clean_fake_news_texts_from_fact_checking=filename_clean_fake_news_texts_from_fact_checking,
                                                   filename_similar_texts_with_fact_checking_texts_primary=filename_similar_texts_with_fact_checking_texts_primary,
                                                   filename_text_from_url_messages_dialogues=filename_text_from_url_messages_dialogues)

        print(">>>>>>>> Running attentionCascades.buildCascadesAttributesFiles method", flush=True)
        filename_fake_news_messages_manual_final = str(DATA_DIRECTORY+"attention_cascades/fake_news_messages_manual_final.tsv")
        attentionCascades.buildCascadesAttributesFiles(filename_cascades_static_attributes=filename_cascades_static_attributes,
                                                  filename_cascades_depth_oriented_attributes=filename_cascades_depth_oriented_attributes,
                                                  filename_cascades_unique_users_over_time=filename_cascades_unique_users_over_time,
                                                  path_graph_sources=path_graph_sources,
                                                   filename_fake_news_messages_manual_final=filename_fake_news_messages_manual_final,
                                                   filename_messages_in_cascades_mapping=filename_messages_in_cascades_mapping,
                                                   filename_messages_in_cascades_mapping_tsv=filename_messages_in_cascades_mapping_tsv,
                                                   filename_text_messages_info=filename_text_messages_info)


        print(">>>>>>>> Running attentionCascades.buildUserRelationCascadeMotifsFile method",
              flush=True)
        filename_user_relations_motifs=str(DATA_DIRECTORY+"attention_cascades/user_cascades_motifs_by_cascade.tsv")
        is_user_cascades_by_cascades = True
        attentionCascades.buildUserRelationCascadeMotifsFile(filename_cascades_static_attributes=filename_cascades_static_attributes,
                                            path_graph_sources=path_user_relations_individual_graph_sources, filename_user_relations_motifs=filename_user_relations_motifs,
                                            is_user_cascades_by_cascades=is_user_cascades_by_cascades)


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
    # pass
    main()
