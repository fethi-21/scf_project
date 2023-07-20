import xml.etree.ElementTree as ET
import pandas as pd
from collections import defaultdict


path_src='D:\Téléchargements\Configuration_scf_MRBTS-504362_5G-69290-006_20230316-1312 (1).xml'
path_trg='D:\Téléchargements\Configuration_scf_MRBTS-506176_5G-75115-030_20230317-1229.xml'


class Scf_compare:
    def __init__(self, path):
        self.root = ET.parse(path).getroot()

    def file_xml(self, classe):
        for i in self.root.findall(".//{raml21.xsd}managedObject/[@class='" + classe + "']"):
            for j in self.root.findall(".//{raml21.xsd}managedObject/[@class='" + classe + "']/[@distName='" + i.attrib[
                'distName'] + "']/{raml21.xsd}list"):
                print(i.attrib)
                for el in self.root.findall(
                        ".//{raml21.xsd}managedObject/[@class='" + classe + "']/[@distName='" + i.attrib[
                            'distName'] + "']/{raml21.xsd}list/[@name='" + j.attrib[
                            "name"] + "']/{raml21.xsd}item/{raml21.xsd}p"):
                    el.attrib["name"] = j.attrib['name'] + "-" + el.attrib["name"]
                    print(el.attrib["name"])

        dicte = defaultdict(list)
        df_lnbts = pd.DataFrame()
        for elm in self.root.findall(".//{raml21.xsd}managedObject/[@class='" + classe + "']"):
            dicte['distName'].append(elm.attrib['distName'])
            dicte['class'].append(elm.attrib['class'])
        for i in self.root.findall(".//{raml21.xsd}managedObject/[@class='" + classe + "']/{raml21.xsd}p"):
            dicte[i.attrib["name"]].append(i.text)
        for j in self.root.findall(
                ".//{raml21.xsd}managedObject/[@class='" + classe + "']/{raml21.xsd}list/{raml21.xsd}item/{raml21.xsd}p"):
            dicte[j.attrib["name"]].append(j.text)
        return pd.DataFrame.from_dict(dicte, orient='index').transpose().fillna(method='ffill')

    def dict_xml(self, liste_class):
        self.src = {}
        """def file_xml_nrcel(self,classe):
            for i in self.root.findall(".//{raml21.xsd}managedObject/[@class='"+classe+"']"):
                for j in self.root.findall(".//{raml21.xsd}managedObject/[@class='"+classe+"']/[@distName='"+i.attrib['distName']+"']/{raml21.xsd}list"):
                    for el in self.root.findall(".//{raml21.xsd}managedObject/[@class='"+classe+"']/[@distName='"+i.attrib['distName']+"']/{raml21.xsd}list/[@name='"+j.attrib["name"]+"']/{raml21.xsd}item/{raml21.xsd}p"):
                        el.attrib["name"]=j.attrib['name']+"-"+el.attrib["name"]
            return None"""
        for i in liste_class:
            self.src["df_" + i] = self.file_xml(i)
        return None

    def convert_df_nrcel(self):
        df = pd.DataFrame()
        for elm in self.root.findall(".//{raml21.xsd}managedObject/[@class='" + classe + "']"):

            dicte = defaultdict(list)
            dicte['distName'].append(elm.attrib['distName'])
            dicte['class'].append(elm.attrib['class'])
            for i in self.root.findall(
                    ".//{raml21.xsd}managedObject/[@class='" + classe + "']/[@distName='" + elm.attrib[
                        'distName'] + "']/{raml21.xsd}p"):
                dicte[i.attrib["name"]].append(i.text)
            for j in self.root.findall(
                    ".//{raml21.xsd}managedObject/[@class='" + classe + "']/[@distName='" + elm.attrib[
                        'distName'] + "']/{raml21.xsd}list/{raml21.xsd}item/{raml21.xsd}p"):
                dicte[j.attrib["name"]].append(j.text)
            df = df.append(pd.DataFrame.from_dict(dicte, orient='index').transpose(), ignore_index=True)

        df = df[df["distName"].notnull()]
        class_ho = {"NOKLTE:CAREL": "lcrId", "NOKLTE:IRFIM": "dlCarFrqEut", "NOKLTE:LNHOW": "utraCarrierFreq",
                    "NOKLTE:REDRT": "redirFreqUtra"}
        df['bande'] = df["distName"].map(lambda x: x.split('/NRCELL-')[-1])

        if classe in list(class_ho.keys()):
            df['bande'] = df['bande'].str[:2] + ">" + df[class_ho[classe]]
        self.df = df.set_index('bande')[df.set_index('bande').index.notnull()]
        return None

    def check_compare(self, other, liste_class):
        check = {}

        def _compare_(self, other):
            other.drop(other[other.index.isin(list(set(self.index).symmetric_difference(other.index)))].index,
                       inplace=True)
            self.drop(self[self.index.isin(list(set(other.index).symmetric_difference(self.index)))].index,
                      inplace=True)
            for i in list(other.columns.difference(self.columns)):
                if other.columns.get_loc(i) > len(self.columns):
                    self[i] = None
                else:
                    self.insert(other.columns.get_loc(i), i, None)

                print(self)
            for i in list(self.columns.difference(other.columns)):
                print(self.columns.get_loc(i))
                if self.columns.get_loc(i) > len(other.columns):
                    other[i] = None
                else:
                    other.insert(self.columns.get_loc(i), i, None)

            self = self[self["distName"].notnull()]
            other = other[other["distName"].notnull()]

            other = other[self.columns]
            return self.compare(other)

        for i in liste_class:
            try:

                check["check_" + i] = _compare_(self.src['df_' + i], other.src['df_' + i])
            except ValueError as e:
                print("Object-" + i + "- doesnt have the same size in src and trg")
                pass
        return check

    def customize_result(check):
        liste_bts = []
        for j in check.keys():
            for i in check[j].index:
                try:
                    print(i)
                    df = check[j][check[j].index == i]
                    df_y = df.transpose().reset_index().pivot(index="level_0", columns="level_1", values=i)
                    df_y["bande"] = i
                    df_y["class"] = j.split(":")[1]
                    liste_bts.append(df_y)
                except:
                    print("Small issue in displaying result for object:" + j)
                    pass
        try:
            df_bts = pd.concat(liste_bts).reset_index()
            # rename columns
            col = ["Parameters", trg_bts, src_bts]
            for i, j in zip(df_bts.columns[:-1], col):
                df_bts.rename(columns={i: j}, inplace=True)
                print(i, j)
            df_bts = df_bts.drop_duplicates(subset=['Parameters', trg_bts, src_bts, 'class'], keep="first")
        except:
            print("result of comparaison is empty or youre comparing the same files")
        return df_bts

file1=Scf_compare(path_src)
file2=Scf_compare(path_trg)

# list class reference
class_ref = ['NRBTS', 'BWP_PROFILE', 'PDCCH', 'PDCCH_CONFIG_COMMON', 'PDSCH', 'PDCCH_CONFIG_DEDICATED', 'NRANR',
             'NRANRPR']

class_ref = ["com.nokia.srbts.nrbts:" + w for w in class_ref]
class_ref.extend(['com.nokia.srbts.tnl:TNLSVC',
                  'com.nokia.srbts.tnl:PMTNLINT', 'com.nokia.srbts.tnl:TNL', 'com.nokia.srbts.tnl:ETHSVC',
                  'com.nokia.srbts.tnl:ETHIF', \
                  'com.nokia.srbts.tnl:VLANIF', 'com.nokia.srbts.tnl:ETHLK', 'com.nokia.srbts.tnl:L2SWI',
                  'com.nokia.srbts.tnl:BRGPRT', \
                  'com.nokia.srbts.tnl:IBRGPRT', 'com.nokia.srbts.tnl:IPNO', 'com.nokia.srbts.tnl:IPIF',
                  'com.nokia.srbts.tnl:IPADDRESSV4', \
                  'com.nokia.srbts.tnl:IPRT', 'com.nokia.srbts.tnl:IPRTV6', 'com.nokia.srbts.tnl:QOS',
                  'com.nokia.srbts.tnl:DSCP2PCPMAP', \
                  'com.nokia.srbts.tnl:DSCP2QMAP', 'com.nokia.srbts.tnl:FSTSCH', 'com.nokia.srbts.tnl:ETHAPP',
                  'com.nokia.srbts.tnl:OAMMD'])
# classes xml_file
src_class = [elm.attrib['class'] for elm in file1.root.findall(".//{raml21.xsd}managedObject")]
trg_class = [elm.attrib['class'] for elm in file2.root.findall(".//{raml21.xsd}managedObject")]

# intersection file
liste_class = [x for x in class_ref if ((x in src_class) and (x in trg_class))]
missing_class = [x for x in class_ref if x not in list(src_class)]

file1.dict_xml(liste_class)
file2.dict_xml(liste_class)

s=file1.check_compare(file2,liste_class)

print(s["check_com.nokia.srbts.nrbts:NRBTS"])
