import argparse
from CampaignManager.lib.helper import execute_command

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--campConfig", type = str, help = "json file for campaign configuration")
parser.add_argument("-a", "--campsToAdd", type = str, nargs = "*", help = "new campaigns added to the config")
parser.add_argument("--dirWmAgentScripts", type = str, default = "/data/unifiedPy3-fast/WmAgentScripts", help = "path to the WmAgentScripts repo")
parser.add_argument("--infoOnly", action='store_true', help = "print out commands to be run without actually running them")

args = parser.parse_args()

if args.campsToAdd:
    for camp in args.campsToAdd:
        execute_command(f"python3 {args.dirWmAgentScripts}/campaignsConfiguration.py --name {camp} --configuration {args.campConfig}", args)

execute_command(f"python3 {args.dirWmAgentScripts}/campaignsConfiguration.py --load {args.campConfig}", args)


