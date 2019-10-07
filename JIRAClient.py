from subprocess import PIPE,Popen
import sys,os,json
import time
from utils import sendLog
import socket

# JIRA requires a peculiar combination of package versions sometimes
# Github issue: https://github.com/CMSCompOps/WmAgentScripts/issues/454
try:
    import jira
except ImportError as e:
    try:
        cmd1 = 'sudo yum remove python-requests python-urllib3 -y' 
        cmd2 = 'sudo pip install --upgrade --force-reinstall requests urllib3'

        print("Error importing jira: {}\nDoing the following commands: \n\t{}\n\t{}".format(e,cmd1, cmd2))
        cmd1out, cmd1err = Popen(cmd1, shell=True, stderr=PIPE, stdout=PIPE).communicate()
        print(cmd1out)
        print(cmd1err)
        cmd2out, cmd2err = Popen(cmd2, shell=True, stderr=PIPE, stdout=PIPE).communicate()
        print(cmd2out)
        print(cmd2err)
        import jira
    except ImportError as e:
        if socket.gethostname().find('.')>=0:
            hostname=socket.gethostname()
        else:
            hostname=socket.gethostbyaddr(socket.gethostname())[0]
        msg = "Error importing jira on {}:\n\t{}".format(hostname, str(e))
        sendLog("jira", msg, level='critical')
        raise e

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
            print "run cern-get-sso-cookie -u https://its.cern.ch/jira/loginCern.jsp -o %s --krb, or something like that"%cookie
        #print cookies
        if not cookies:
            print "That ain't going to work out"
            sys.exit(1)
        self.client = jira.JIRA('https://its.cern.ch/jira' , options = {'cookies':  cookies})

    def last_time(self, j):
        try:
            j = self.get(j.key)
        except:
            j = self.get(j)
        last_comment_time = self.time_to_time(j.fields.comment.comments[-1].updated) if (hasattr(j.fields, 'comment') and j.fields.comment.comments) else self.created(j)
        return last_comment_time

    def create_or_last(self, prepid, priority=None, label=None, reopen=False):
        jiras = self.find( {'prepid' : prepid})
        j = None
        created = False
        reopened = False
        if len(jiras)==0:
            c_doc = { 'summary' : prepid,
                      'description' : 'https://dmytro.web.cern.ch/dmytro/cmsprodmon/workflows.php?prep_id=%s \nAutomatic JIRA from unified'%( prepid )}
            if priority: c_doc['priority'] = priority
            if label: c_doc['label'] = label
            j = self.create(c_doc)
            created = True
        else:
            j = sorted(jiras, key= lambda o:self.created(o))[-1]
            if reopen:
                reopened = self.reopen( j.key )
        return j,reopened,created

    def create(self , indications , do = True):
        fields = {
            'project' : 'CMSCOMPPR',
            'issuetype' : {'id' : "3"}
            }
        label = indications.get('label',None)
        who = {
            'WorkflowTrafficController' : 'sagarwal',
            'UnifiedOfficer' : 'sagarwal',
            'AgentDoc' : 'jen_a',
            'Late' : None
        }.get(label,None)
           
        if label:
            fields['labels'] = [ label ]
        if who:
            fields['assignee'] = {'name':who,'key':who}
        summary = indications.get('summary',None)
        if summary:
            fields['summary'] = summary
        description = indications.get('description', None)
        if description:
            fields['description'] = description
        priority = indications.get('priority', None)
        if type(priority) == int:
            ## transform to a str
            #needs decision : 6
            #blocker : 1
            #critical : 2
            #major : 3
            #minor : 4

            prios = {
                0  : '4',
                85000  : '3',
                110000 : '2'
            }
            
            set_to = None
            for p in sorted(prios.keys()):
                if priority>=p:
                    set_to = prios[p]
            priority = set_to

        elif (priority and not priority.isdigit()):
            priority = { 'decision' : '6',
                         'blocker' : '1'}.get(priority, None)
        if priority:
            fields['priority'] = {'id' : priority}

        print fields
        if do:
            i = self.client.create_issue( fields) 
            return i

    def find(self ,specifications):
        query  = 'project=CMSCOMPPR'
        summary = specifications.get('prepid',specifications.get('summary',None))
        if summary:
            query += ' AND summary~"%s"'%(summary)

        if specifications.get('status',None):
            status = specifications['status']
            if status.startswith('!'):
                query += ' AND status != %s'%(status[1:])
            else:
                query += ' AND status = %s'%(status)

        if specifications.get('label'):
            label = specifications['label']
            query += ' AND labels = %s'%label

        return self._find( query )

    def comment(self, key, comment):
        if not comment: return
        self.client.add_comment( key, comment )

    def _find(self, query):
        return self.client.search_issues( query ,maxResults=-1)

    def get(self, jid):
        return self.client.issue( jid )

    def time_to_time(self, time_str):
        ts,aux = time_str.split('.')
        if '+' in aux:
            s = int(aux.split('+')[-1])/100.
        else:
            s = -int(aux.split('-')[-1])/100.
        t = time.mktime(time.strptime( ts, "%Y-%m-%dT%H:%M:%S")) - s*60*60
        return t

    def created(self,j):
        return self.time_to_time( j.fields.created )

    def _transition(self, status, jid):
        to = {'closed' : '2',
              'reopened' : '3',
              'progress' : '4'
              }.get( status , None)
        if to:
            try:
                print jid,"to",status
                self.client.transition_issue( jid, to)
                return True
            except Exception as e:
                print "transition to",status,"not successful"
                print str(e)
                return False
        else:
            print "transition to",status,"not known"
        return False

    def progress(self, jid):
        return self._transition('progress', jid)

    def reopen(self, jid):
        return self._transition('reopened', jid)

    def close(self, jid):
        return self._transition('closed', jid)

if __name__ == "__main__":
    JC = JIRAClient(cookie = 'jira.txt')
    
    i= JC.get('CMSCOMPPR-4516')
    print i.fields.summary

    ii = JC.find({'prepid' : 'SUS-RunIISummer16MiniAODv3-00261'})
    print [io.key for io in ii]

    #JC.reopen('CMSCOMPPR-4518')
    #JC.progress('CMSCOMPPR-4518')
    #JC.close('CMSCOMPPR-4518')

    #JC.create( {
    #    'priority' : 120000,
    #    'summary' : 'automatic',
    #    'label' : 'WorkflowTrafficController',
    #    'description' : 'Automatic JIRA from unified'},
    #           do = False)
    
    ii = JC.find({'summary' : 'vocms0253.cern.ch heartbeat issues'})

    print [time.asctime(time.gmtime(JC.created(io))) for io in sorted(ii, key=lambda o:JC.created(o))]
