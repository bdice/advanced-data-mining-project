import glob
import itertools

from . import BuildRulesFastParameterFree
from . import BuildRulesFastParameterFreeFreq
from . import BuildNetwork

## Initialize algorithm parameters
MaxOrder = 99
MinSupport = 10

OutputRulesFile = '../data/rules-higher-order-airports.csv'
OutputNetworkFile = '../data/network-higher-order-airports.csv'

LastStepsHoldOutForTesting = 0
MinimumLengthForTraining = 1
InputFileDeliminator = ' '
Verbose = True


###########################################
# Functions
###########################################

def ReadSequentialData(InputFileName):
    if Verbose:
        print('Reading raw sequential data')
    RawTrajectories = []

    for filename in glob.glob(InputFileName):
        with open(filename) as f:
            LoopCounter = 0
            for line in f:
                fields = line.strip().split(InputFileDeliminator)
                ## In the context of global shipping, a ship sails among many ports
                ## and generate trajectories.
                ## Every line of record in the input file is in the format of:
                ## [Ship1] [Port1] [Port2] [Port3]...
                ship = fields[0]
                movements = fields[1:]

                LoopCounter += 1
                if LoopCounter % 10000 == 0:
                    VPrint(LoopCounter)

                ## Other preprocessing or metadata processing can be added here

                ## Test for movement length
                MinMovementLength = MinimumLengthForTraining + LastStepsHoldOutForTesting
                if len(movements) < MinMovementLength:
                    continue

                RawTrajectories.append([ship, movements])


        return RawTrajectories


def BuildTrainingAndTesting(RawTrajectories):
    VPrint('Building training and testing')
    Training = []
    Testing = []
    for trajectory in RawTrajectories:
        ship, movement = trajectory
        movement = [key for key,grp in itertools.groupby(movement)] # remove adjacent duplications
        if LastStepsHoldOutForTesting > 0:
            Training.append([ship, movement[:-LastStepsHoldOutForTesting]])
            Testing.append([ship, movement[-LastStepsHoldOutForTesting]])
        else:
            Training.append([ship, movement])
    return Training, Testing

def DumpRules(Rules, OutputRulesFile):
    VPrint('Dumping rules to file')
    with open(OutputRulesFile, 'w') as f:
        for Source in Rules:
            for Target in Rules[Source]:
                f.write(' '.join([' '.join([str(x) for x in Source]), '=>', Target, str(Rules[Source][Target])]) + '\n')

def DumpNetwork(Network, OutputNetworkFile):
    VPrint('Dumping network to file')
    LineCount = 0
    with open(OutputNetworkFile, 'w') as f:
        for source in Network:
            for target in Network[source]:
                f.write(','.join([SequenceToNode(source), SequenceToNode(target), str(Network[source][target])]) + '\n')
                LineCount += 1
    VPrint(str(LineCount) + ' lines written.')

def SequenceToNode(seq):
    curr = seq[-1]
    node = curr + '|'
    seq = seq[:-1]
    while len(seq) > 0:
        curr = seq[-1]
        node = node + curr + '.'
        seq = seq[:-1]
    if node[-1] == '.':
        return node[:-1]
    else:
        return node

def VPrint(string):
    if Verbose:
        print(string)


def BuildHON(InputFileName, OutputNetworkFile):
    RawTrajectories = ReadSequentialData(InputFileName)
    TrainingTrajectory, TestingTrajectory = BuildTrainingAndTesting(RawTrajectories)
    VPrint(len(TrainingTrajectory))
    Rules = BuildRulesFastParameterFree.ExtractRules(TrainingTrajectory, MaxOrder, MinSupport)
    # DumpRules(Rules, OutputRulesFile)
    Network = BuildNetwork.BuildNetwork(Rules)
    DumpNetwork(Network, OutputNetworkFile)
    VPrint('Done: '+InputFileName)

def BuildHONfreq(InputFileName, OutputNetworkFile):
    print('FREQ mode!!!!!!')
    RawTrajectories = ReadSequentialData(InputFileName)
    TrainingTrajectory, TestingTrajectory = BuildTrainingAndTesting(RawTrajectories)
    VPrint(len(TrainingTrajectory))
    Rules = BuildRulesFastParameterFreeFreq.ExtractRules(TrainingTrajectory, MaxOrder, MinSupport)
    # DumpRules(Rules, OutputRulesFile)
    Network = BuildNetwork.BuildNetwork(Rules)
    DumpNetwork(Network, OutputNetworkFile)
    VPrint('Done: '+InputFileName)


def generate(input_filename, output_rules_file, output_network_file):
    RawTrajectories = ReadSequentialData(input_filename)
    TrainingTrajectory, TestingTrajectory = BuildTrainingAndTesting(RawTrajectories)
    VPrint(len(TrainingTrajectory))
    Rules = BuildRulesFastParameterFreeFreq.ExtractRules(TrainingTrajectory, MaxOrder, MinSupport)
    DumpRules(Rules, output_rules_file)
    Network = BuildNetwork.BuildNetwork(Rules)
    DumpNetwork(Network, output_network_file)
