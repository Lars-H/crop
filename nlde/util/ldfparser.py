'''
Created on 01.10.2018

@author: maribelacosta
'''


def LDFParser(template, answers, var, queue, server, count, context):

    card = 0

    if template == 4:
        card = parseVarS(answers, var, queue, server, count, context)
    elif template == 2:
        card = parseVarP(answers, var, queue, server, count, context)
    elif template == 1:
        card = parseVarO(answers, var, queue, server, count, context)
    elif template == 6:
        card = parseVarSP(answers, var, queue, server, count, context)
    elif template == 5:
        card = parseVarSO(answers, var, queue, server, count, context)
    elif template == 3:
        card = parseVarPO(answers, var, queue, server, count, context)
    elif template == 0:
        card = parseNoVar(answers, var, queue, server, count, context)

    return card


def parseNoVar(answers, var, queue, total, count, context):
        queue.put({})
        return 0


def parseVarS(answers, var, queue, server, count, context):

    card = 0
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():
            pos = elem["@id"].find(":")
            prefix = elem["@id"][0:pos]
            if prefix in context.keys():
                elem["@id"] = elem["@id"].replace(prefix+":", context[prefix])
            card = card + 1
            queue.put({var[0]: elem["@id"]})

    return card
            
def parseVarP(answers, var, queue, server, count, context):

    card = 0
    to_process = {}
    for elem in answers:

        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():
            to_process = elem

    if to_process == {}:
        return card

    to_process = to_process.keys()
    to_process.remove("@id")

    for elem in to_process:
        pos = elem.find(":")
        if pos != -1:
            prefix = elem[0:pos]
            if prefix in context.keys():
                elem = elem.replace(prefix + ":", context[prefix])
        else:
            prefix = p
            if prefix == "@type":
                p = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            if "http" not in elem:
                if prefix in context.keys():
                    elem = context[prefix]["@id"]
        card = card + 1
        queue.put({var[0]: elem})

    return card

def parseVarO(answers, var, queue, server, count, context):

    card = 0
    to_process = {}
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():
        #if "@graph" not in elem.keys() and "hydra" not in str(elem) and "void" not in str(elem) and "variable" not in str(elem) and "template" not in str(elem):
            to_process = elem

    if to_process == {}:
        return card

    to_process.pop("@id", None)
    p = to_process.keys()[0]
    answers = to_process

    if isinstance(answers[p], dict):
        elem = answers[p]
        if "@id" in elem.keys():
            o = elem["@id"]
            pos = o.find(":")
            prefix = o[0:pos]
            if prefix in context.keys():
                o = o.replace(prefix + ":", context[prefix])

        else:
            o = elem["@value"]
            if "@language" in elem.keys():
                o = '"' + o + '"' + "@" + elem["@language"]
            elif "@type" in elem.keys():
                pos = elem["@type"].find(":")
                prefix = elem["@type"][0:pos]
                if prefix in context.keys():
                    elem["@type"] = elem["@type"].replace(prefix + ":", context[prefix])

                o = '"' + o + '"' + "^^" + "<" + elem["@type"] + ">"

        card = card + 1
        queue.put({var[0]: o})
   
    elif not(isinstance(answers[p], list)):
        elem = answers[p]
        if isinstance(elem, int):
            elem = '"' + str(elem) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
        card = card + 1
        queue.put({var[0]: elem})
        return card

    else:
        for elem in answers[p]:

            if isinstance(elem, dict):
                if "@id" in elem.keys():
                    o = elem["@id"]
                    pos = o.find(":")
                    if pos != -1:
                        prefix = o[0:pos]
                        if prefix in context.keys():
                            o = o.replace(prefix + ":", context[prefix])
                    else:
                        if "http" not in o:
                            prefix = o
                            o = context[prefix]["@id"]

                else:
                    o = elem["@value"]
                    if "@language" in elem.keys():
                        o = '"' + o + '"' + "@" + elem["@language"]
                    elif "@type" in elem.keys():
                        pos = elem["@type"].find(":")
                        prefix = elem["@type"][0:pos]
                        if prefix in context.keys():
                            elem["@type"] = elem["@type"].replace(prefix + ":", context[prefix])

                        o = '"' + o + '"' + "^^" + "<" + elem["@type"] + ">"
                card = card + 1
                queue.put({var[0]: o})

            else:
                if isinstance(elem, int):
                    elem = '"' + str(elem) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
                card = card + 1
                queue.put({var[0]: elem})

    return card


def parseVarSP(answers, var, queue, server, count, context):

    card = 0
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():
            s = elem["@id"]
            pos = s.find(":")
            prefix = s[0:pos]
            if prefix in context.keys():
                s = s.replace(prefix + ":", context[prefix])

            pred = elem.keys()
            pred.remove("@id")
            for p in pred:
                pos = p.find(":")
                if pos != -1:
                    prefix = p[0:pos]
                    if prefix in context.keys():
                        p = p.replace(prefix + ":", context[prefix])
                else:
                    prefix = p
                    if prefix == "@type":
                        p = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                    elif "http" not in p:
                        if prefix in context.keys():
                            p = context[prefix]["@id"]
                card = card + 1
                queue.put({var[0]: s, var[1]: p})

    return card



def parseVarSO(answers, var, queue, server, count, context):

    card = 0
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():
            s = elem["@id"]
            pos = s.find(":")

            if pos != -1:
                prefix = s[0:pos]
                if prefix in context.keys():
                    s = s.replace(prefix + ":", context[prefix])
            else:
                prefix = s
                if "http" not in prefix:
                    s = context[prefix]["@id"]

            del elem["@id"]
            p = elem.keys()[0]

            if isinstance(elem[p], dict):
                if "@id" in elem[p].keys():
                    o = elem[p]["@id"]
                    pos = o.find(":")

                    if pos != -1:
                        prefix = o[0:pos]
                        if prefix in context.keys():
                            o = o.replace(prefix + ":", context[prefix])
                    else:
                        prefix = o
                        if "http" not in prefix:
                            o = context[prefix]["@id"]

                else:
                    o = elem[p]["@value"] 
                    if "@language" in elem[p].keys():
                        o = '"' + o + '"' + "@" + elem[p]["@language"]
                    elif "@type" in elem[p].keys():
                        pos = elem[p]["@type"].find(":")
                        prefix = elem[p]["@type"][0:pos]
                        if prefix in context.keys():
                            elem[p]["@type"] = elem[p]["@type"].replace(prefix + ":", context[prefix])
                        o = '"' + o + '"' + "^^" + "<" + elem[p]["@type"] + ">"
                    elif isinstance(o, int):
                        o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"

                card = card + 1
                queue.put({var[0]: s, var[1]: o})
                
            elif isinstance(elem[p], list):
                for oelem in elem[p]:
                    if isinstance(oelem, dict):
                        if "@id" in oelem.keys():
                            o = oelem["@id"]
                            pos = o.find(":")

                            if pos != -1:
                                prefix = o[0:pos]
                                if prefix in context.keys():
                                    o = o.replace(prefix + ":", context[prefix])
                            else:
                                prefix = o
                                if "http" not in prefix:
                                    o = context[prefix]["@id"]

                        else:
                            o = oelem["@value"]
                            if "@language" in oelem.keys():
                                o = '"' + o + '"' + "@" + oelem["@language"]
                            elif "@type" in oelem.keys():
                                pos = oelem["@type"].find(":")
                                if pos != -1:
                                    prefix = oelem["@type"][0:pos]
                                    if prefix in context.keys():
                                        oelem["@type"] = oelem["@type"].replace(prefix + ":", context[prefix])
                                else:
                                    prefix = oelem["@type"]
                                    if "http" not in prefix:
                                        oelem["@type"] = context[prefix]["@id"]

                                o = '"' + o + '"' + "^^" + "<" + oelem["@type"] + ">"

                            elif isinstance(o, int):
                                o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
                        card = card + 1
                        queue.put({var[0]: s, var[1]: o})

                    else:
                        if isinstance(oelem, int):
                            oelem = '"' + str(oelem) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
                        card = card + 1
                        queue.put({var[0]: s, var[1]: oelem})
            
            else:
                if isinstance(elem[p], int):
                    elem[p] = '"' + str(elem[p]) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"
                card = card + 1
                queue.put({var[0]: s, var[1]: elem[p]})

    return card
                    
                    
def parseVarPO(answers, var, queue, server, count, context):

    card = 0
    for elem in answers:
        if server not in str(elem) and "hydra" not in str(elem) and "variable" not in elem.keys():

            for p in elem.keys():
                if p == "@id":
                    continue

                p_expanded = None
                pos = p.find(":")
                if pos != -1:
                    prefix = p[0:pos]
                    if prefix in context.keys():
                        p_expanded = p.replace(prefix + ":", context[prefix])

                else:
                    prefix = p
                    if prefix == "@type":
                        p_expanded = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                    elif "http" not in prefix:
                        if isinstance(context[prefix], dict):
                            p_expanded = context[prefix]["@id"]
                        else:
                            p_expanded = context[prefix]

                if p_expanded is None:
                    p_expanded = p

                if isinstance(elem[p], dict):

                    if "@id" in elem[p].keys():
                        o = elem[p]["@id"]
                        pos = o.find(":")

                        if pos != -1:
                            prefix = o[0:pos]
                            if prefix in context.keys():
                                o = o.replace(prefix + ":", context[prefix])
                        else:
                            prefix = o
                            if "http" not in prefix:
                                o = context[prefix]["@id"]
                    else:
                        o = elem[p]["@value"]
                        if "@language" in elem[p].keys():
                            o = '"' + o + '"' + "@" + elem[p]["@language"]
                        elif "@type" in elem[p].keys():
                            pos = elem[p]["@type"].find(":")
                            prefix = elem[p]["@type"][0:pos]
                            if prefix in context.keys():
                                elem[p]["@type"] = elem[p]["@type"].replace(prefix + ":", context[prefix])
                            o = '"' + o + '"' + "^^" + "<" + elem[p]["@type"] + ">"
                        elif isinstance(o, int):
                            o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"

                    card = card + 1
                    queue.put({var[0]: p_expanded, var[1]: o})

                elif isinstance(elem[p], list):
                    for oelem in elem[p]:
                        if isinstance(oelem, dict):
                            if "@id" in oelem.keys():
                                o = oelem["@id"]
                                pos = o.find(":")
                                prefix = o[0:pos]
                                if prefix in context.keys ():
                                    o = o.replace(prefix + ":", context[prefix])

                            else:
                                o = oelem["@value"]
                                if "@language" in oelem.keys():
                                    o = '"' + o + '"' + "@" + oelem["@language"]
                                elif "@type" in oelem.keys():
                                    pos = oelem["@type"].find(":")
                                    prefix = oelem["@type"][0:pos]
                                    if prefix in context.keys():
                                        oelem["@type"] = oelem["@type"].replace (prefix + ":", context[prefix])
                                    o = '"' + o + '"' + "^^" + "<" + oelem["@type"] + ">"
                                elif isinstance(o, int):
                                    o = '"' + str(o) + '"' + "^^" + "<http://www.w3.org/2001/XMLSchema#integer>"

                            card = card + 1
                            queue.put({var[0]: p_expanded, var[1]: o})

                        else:
                            card = card + 1
                            queue.put({var[0]: p_expanded, var[1]: oelem})

                else:
                    card = card + 1
                    queue.put({var[0]: p_expanded, var[1]: elem[p]})

    return card

def isliteral(s):
    if (isinstance(s, str)) and (len(s) > 0) and (s[0] == '"'):
        return True
    else:
        return False


def replace_prefix(uri, prefix, path):
    return "<" + uri.replace(prefix+":", path) + ">"

