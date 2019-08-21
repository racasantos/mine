from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport
import pika

class RertrieveFDG:

    def __init__(self, user_name, password,erp_integration_wsdl):
        self.user_name = user_name
        self.password = password
        self.erp_integration_wsdl = erp_integration_wsdl

        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='fdg')

        session = Session()

        session.auth = HTTPBasicAuth(self.user_name, self.password)

        client = Client(self.erp_integration_wsdl, transport=Transport(session=session))
        fdg = client.service.getDocumentsForFilePrefix('%XML_Nfe_%','fin$/receivables$/import$','')
        i=0
        while i<len(fdg):
            xml_fdg = fdg[i].Content.decode("utf-8")
            channel.basic_publish(exchange='',
                          routing_key='fdg',
                          body=xml_fdg)
            print(xml_fdg)
            i += 1
            connection.close()
