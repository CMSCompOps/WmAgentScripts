import sys
import os
import time

def main():

    secrets_file=open("/home/relval/secrets.txt")

    passwords=secrets_file.read().rstrip('\n')

    relval_password = passwords.split('\n')[0]
    voms_proxy_password = passwords.split('\n')[1]

    ret=os.system("kinit << EOF\n"+relval_password+"\nEOF")

    os.system("aklog")

    #sometimes the kerberos initialization doesn't work temporarily
    while ret != 0:
        ret=os.system("kinit << EOF\n"+relval_password+"\nEOF")
        os.system("aklog")
        time.sleep(60)

    ret=os.system("voms-proxy-init -voms cms  --valid 192:00 << EOF\n"+voms_proxy_password+"\nEOF")

    #time.sleep(3600)
    #sys.exit(0)
    
#curs.execute("insert into batches set hn_req=\""+hnrequest+"\", announcement_title=\"myannouncementtitle\"")

if __name__ == "__main__":
    main()

