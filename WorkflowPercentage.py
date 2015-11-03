#!/usr/bin/env python
import sys
import re
import reqMgrClient
from optparse import OptionParser
"""
    Calculates event progress percentage of a given workflow,
    taking into account the workflow type, and comparing
    input vs. output numbers of events.
    Should be used instead of dbsTest.py
    TODO MC??
   
"""


def percentageCompletion(url, workflow, verbose=False, checkLumis=False, checkFilter=False, skipInvalid=False):
    """
    Calculates Percentage of completion for a given workflow
    taking a particular output dataset
    if checkLumis is enabled, we get lumis instead.
    """
    # input events/lumis
    try:
        if checkLumis:
            inputEvents = workflow.getInputLumis()
        else:
            inputEvents = workflow.getInputEvents()
    except:
        # no input dataset
        inputEvents = 0

    # filter Efficiency (only for events)
    if checkFilter and not checkLumis and 'FilterEfficiency' in workflow.info:
        filterEff = workflow.filterEfficiency
    else:
        filterEff = 1.0
    # datasets
    for dataset in workflow.outputDatasets:
        # output events/lumis
        if checkLumis:
            outputEvents = workflow.getOutputLumis(dataset, skipInvalid)
        else:
            outputEvents = workflow.getOutputEvents(dataset, skipInvalid)
        if not outputEvents:
            outputEvents = 0
        # calculate percentage
        if not inputEvents:
            perc = 0
        else:
            perc = 100.0 * outputEvents / float(inputEvents) / filterEff
        # print results
        if verbose:
            print dataset
            print "Input %s: %d" % ("lumis" if checkLumis else "events", int(inputEvents))
            print ("Output %s: %d (%s%%)" % ("lumis" if checkLumis else "events", int(outputEvents), perc) +
                   ('(filter=%s)' % filterEff if checkFilter and not checkLumis else ''))
        else:
            print dataset, "%s%%" % perc


def percentageCompletion2StepMC(url, workflow, verbose=False, checkLumis=False):
    """
    Calculates percentage completion for a MonteCarlo
    with GEN and GEN-SIM output
    pdmvserv_SMP-Summer14Test2wmGENSIM-00002_00002_v0__140831_173202_4712
    """
    # input events/lumis
    try:
        if checkLumis:
            inputEvents = workflow.getInputLumis()
        else:
            inputEvents = workflow.getInputEvents()
    except:
        # no input dataset
        inputEvents = 0

    # filter Efficiency (only for events)
    if not checkLumis and 'FilterEfficiency' in workflow.info:
        filterEff = workflow.filterEfficiency
    else:
        filterEff = 1.0

    # set the GEN first
    if re.match('.*/GEN$', workflow.outputDatasets[1]):
        workflow.outputDatasets = [
            workflow.outputDatasets[1], workflow.outputDatasets[0]]
    # output events/lumis
    if checkLumis:
        outputEvents = [workflow.getOutputLumis(workflow.outputDatasets[0]),
                        workflow.getOutputLumis(workflow.outputDatasets[1])]
    else:
        outputEvents = [workflow.getOutputEvents(workflow.outputDatasets[0]),
                        workflow.getOutputEvents(workflow.outputDatasets[1])]
    if not inputEvents:
        perc = [100.0, 100.0 * outputEvents[1] / outputEvents[0]]
    else:
        perc = [100.0 * outputEvents[0] / float(inputEvents),
                100.0 * outputEvents[1] / float(inputEvents) / filterEff]
    # print results
    if verbose:
        print "Input %s: %d" % ("lumis" if checkLumis else "events", int(inputEvents))
        print workflow.outputDatasets[0]
        print "Output %s: %d (%s%%)" % ("lumis" if checkLumis else "events", int(outputEvents[0]), perc[0])
        print workflow.outputDatasets[1]
        print ("Output %s: %d (%s%%)" % ("lumis" if checkLumis else "events", int(outputEvents[1]), perc[1]) +
               ('(filter=%s)' % filterEff if not checkLumis else ''))
    else:
        print workflow.outputDatasets[0], "%s%%" % perc[0]
        print workflow.outputDatasets[1], "%s%%" % perc[1]


def percentageCompletionTaskChain(url, workflow, verbose=False, checkLumis=False, skipInvalid=False):
    """
    Calculates a Percentage completion for a taskchain.
    Taking step/filter efficiency into account.
    pdmvserv_task_SUS-Summer12WMLHE-00004__v1_T_141003_120119_9755
    """
    workflow = reqMgrClient.TaskChain(workflow.name)
    if checkLumis:
        inputEvents = workflow.getInputLumis()
    else:
        inputEvents = workflow.getInputEvents()
    if verbose:
        print "Input %s:" % ("lumis" if checkLumis else "events"), inputEvents
    i = 1

    # task-chain 1, starts with GEN or LHE, a GEN-SIM, GEN-SIM-RAW, AODSIM,
    # DQM and so on
    for dataset in workflow.outputDatasets:
        if verbose:
            print dataset
        if not checkLumis:
            outputEvents = workflow.getOutputEvents(dataset, skipInvalid)
        else:
            outputEvents = workflow.getOutputLumis(dataset)
        # GEN or LHE and GEN-SIM and events, we take into account filter
        # efficiency
        if 1 <= i <= 2 and not checkLumis:
            filterEff = workflow.getFilterEfficiency('Task%d' % i)
            # decrease filter eff
            inputEvents *= filterEff
            percentage = 100.0 * outputEvents / \
                float(inputEvents) if inputEvents > 0 else 0.0
            if verbose:
                print "Output %s:" % ("lumis" if checkLumis else "events"), int(outputEvents), "(%.2f%%)" % percentage
        # Other datasets, or lumis, we ignore filter efficiency
        else:
            percentage = 100.0 * outputEvents / \
                float(inputEvents) if inputEvents > 0 else 0.0
            if verbose:
                print "Output %s:" % ("lumis" if checkLumis else "events"), int(outputEvents), "(%.2f%%)" % percentage
        if not verbose:
            print dataset, "%s%%" % percentage
        i += 1

url = 'cmsweb.cern.ch'


def main():
    usage = "usage: %prog [options] workflow"
    parser = OptionParser(usage=usage)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                      help="Show detailed info")
    parser.add_option("-l", "--lumis", action="store_true", dest="checkLumis", default=False,
                      help="Show lumis instead of events")
    parser.add_option("-k", "--skip", action="store_true", dest="skipInvalid", default=False,
                      help="Skip invalid files in the output dataset (may be slower)")
    parser.add_option("-f", "--file", dest="fileName", default=None,
                      help="Input file")
    (options, args) = parser.parse_args()

    if len(args) != 1 and options.fileName is None:
        parser.error("Provide the workflow name or a file")
        sys.exit(1)
    if options.fileName is None:
        workflows = [args[0]]
    else:
        workflows = [l.strip() for l in open(options.fileName) if l.strip()]

    for wf in workflows:
        print wf
        workflow = reqMgrClient.Workflow(wf, url)
        # by type
        if workflow.type != 'TaskChain':
            # two step monte carlos (GEN and GEN-SIM)
            if workflow.type == 'MonteCarlo' and len(workflow.outputDatasets) == 2:
                percentageCompletion2StepMC(
                    url, workflow, options.verbose, options.checkLumis)
            elif workflow.type == 'MonteCarloFromGEN':
                percentageCompletion(url, workflow, options.verbose,
                                     options.checkLumis, checkFilter=True, skipInvalid=options.skipInvalid)
            else:
                percentageCompletion(url, workflow, options.verbose,
                                     options.checkLumis, checkFilter=True, skipInvalid=options.skipInvalid)
        else:
            percentageCompletionTaskChain(
                url, workflow, options.verbose, options.checkLumis)

if __name__ == "__main__":
    main()
