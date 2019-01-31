import sys,os,json
import jira

class JIRAClient:
    def __init__(self, debug=False,cookie=None):
        self.server='https://its.cern.ch/jira'

        cookie = os.environ.get('JIRA_SSO_COOKIE', cookie)
        cookies = {}
        try:
            print "using cookie from", cookie
            for l in open(cookie,'r').read().split('\n'):
                try:
                    s = l.split()
                    if s[5] in ['JSESSIONID','atlassian.xsrf.token']:
                        cookies[ s[5] ] = s[6]
                except:
                    pass
        except:
            print cookie,"is not a file?"
            print "run cern-get-sso-cookie -u https://its.cern.ch/jira/loginCern.jsp -o jira.txt, or something like that"
        #print cookies
        if not cookies:
            print "That ain't going to work out"
            sys.exit(1)
        self.client = jira.JIRA('https://its.cern.ch/jira' , options = {'cookies':  cookies})
    
    def create(self , indications):
        fields = {
            'project' : 'CMSCOMPPR',
            'issuetype' : {'id' : "3"}
            }
        fields.update(
            {
                'summary' : '<prepid> issue',
                'priority' : {'id' : "3"},
                'assignee' : {'name':'vlimant', 'key':'vlimant'},
                'labels' : ['WorkflowTrafficController']
            }
        )
        i = self.client.create_issue( fields) 
        return i

    def find(self ):
        pass

    def get(self, jid):
        return self.client.issue( jid )



if __name__ == "__main__":
    JC = JIRAClient(cookie = 'jira.txt')
    
    i= JC.get('CMSCOMPPR-4516')
    print i.fields.summary
