#-------------------------------------------------------------------IMPORTING REQUIRED LIBRARIES---------------------------------------------------------

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from email.mime.application import MIMEApplication

import openpyxl as op
#------------------------------------------------------------------EMAIL HANDLING----------------------------------------------------------------------
#Loading the Validated Tables DF

tble_list=spark.read.format('csv')\
		.option("header","true")\
		.load('/dbfs/teradata/tble.csv')

display(tble_list)


#Email Parameters

mail_subject='Data-Validation'
mail_server='<Enter the Server>'
mail_port='<Enter port>'
server_auth=True
mail_from_name="DataValidation"
starttls_enable=True
mail_sender_email=[""]
mail_recipents=[""]
mail_cc=[]
mail_priority="1"
mail_body_begin='''<!DOCTYPE html>
				<html>
				<head>
					<style>
						body {
							font-familty: Arial,Helvetica Neue, Helvetica, sans-serif;
							color: #555;
						}
						table {
							border-collapse: collapse;
							width: 100%
						}
						th {
							color : white;
						}
						td, th{
							border: 1px solid #dddddd;
							text-align: left,
							padding: 8px;
						}
						h1, h4 {
							font-size: 20px;
							font-weight: 700;
							letter-spacing: normal;
							line-height: 120%
							text-align: left;
							margin-top: 10px;
							margin-bottom: 20px;
						}
						p {
							font-size: 16px;
						}
						li {
							font-size: 15px;
						}
						h4 {
							font-size: 15px;
							margin-bottom: 5px;
						}
						</style>
						</head>
						<body>
							<h1>Data Validation Table Details</h1>
						'''


def send_mail(mail_body):
	body=mail_body
	server = smtplib.SMTP(mail_server, mail_port)
	msge=MIMEMultipart()
	msge["From"] = mail_from_name
	msge["To"] =",".join(mail_recipents)
	msge["Subject"]=mail_subject
	msge["X-Priority"]=mail_priority
	msge.attach(MIMEText(body,"html"))
	with open('/tmp/data_validation.xlsx',"rb") as attach:
		part=MIMEBase("application","octet-stream")
		part.set_payload((attach).read())
	encoders.encode_base64(part)
	part.add_header(
		"Content-Despositon",
		f"attachment; filename=Data_Validation_Report.xlsx"
	)
	msge.attach(part)
	msge_txt=msge.as_string()
	server.sendmail(mail_sender_email,mail_recipents,msge_txt)
	server.quit()

#Mail Body Details

def email_body(tble_det):
	body_m=mail_body_begin + "<h4>List of records that were successfully validated:</h4>"
	table ="<table><tr style='background: #20B2AA;'><th> Teradata Table Name</th><th>Target Table</th></tr>"
	#create dynamic HTML table with good records
	for r in tble_det.collect():
		teradata_table=str(r['Teradata Table Name'])
		target_table=str(r['TargetTableName'])
		row=f"""<tr><td>{teradata_table}</td>
				<td>{target_table}</td></tr>"""
		table=table+row
	body_m=body_m + table + "</table>"
	return body_m


#Send Mail

message_body=email_body(tble_list)

print(message_body)

send_mail(message_body)

#Remove prcoessed table details

dbutils.fs.rm('/dbfs/teradata',True)


#Checking of report file deleted or not

%sh

ls '/tmp/' | grep 'data_validation.xlsx' | wc -l

rm -f '/tmp/data_validation.xlsx'

n1=$(ls -l '/tmp/' | grep 'data_validation.xlsx' | wc -l)
if test $n1 -eq 0
then
echo 'Temp File Removed Successfully'
else
echo 'There is a file in Temp Directory'
fi

ls '/tmp/' | grep 'data_validation.xlsx' | wc -l


#Creating Excel File for next Cycle

wb=op.Workbook()
wb.save('tmp/data_validation.xlsx')