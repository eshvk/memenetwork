import sys
import re

def generate_trigram(index, length, tokens):
    trigram = ''
    while length!= 0:
        if trigram == '':
            trigram = tokens[index] + '\t' + tokens[index+1] + '\t' +  \
                                tokens[index+2]
            index = index + 3
        else:
            trigram += '\t' + tokens[index]
            index = index + 1
        length = length - 1
    return trigram

def generate_tokens(f1):
    cleaned_tokens = []
    currline = f1.readline()
    while ((currline)!= ''):
        tokens = currline.split()
        cleaned_tokens += [re.sub(r'\W+', '', token) for token in tokens]
        currline = f1.readline()
    return cleaned_tokens

def generate_trigram_alignments(f2):
    trigram_alignments = {}
    currline = f2.readline()
    while ((currline)!= ''):
        relevantline = currline.partition('\t')[2]
        pairs = relevantline.split('\t')
        print len(pairs)
        for p in pairs:
            p = p.strip('[]')
            temp = p.partition('(')
            v = temp[1] + temp[2]
            temp2 = temp[0].split(',')
            k = (int(temp2[0]), int(temp2[1]))
            if k in trigram_alignments:
                temp3 = trigram_alignments[k]
                temp3.append(v)
                trigram_alignments[k] = temp3
            else:
                trigram_alignments[k] = [v]
        currline = f2.readline()
    return trigram_alignments

def main():
    f1 = open(sys.argv[1], 'r')
    f2 = open(sys.argv[2], 'r')
    f3 = open(sys.argv[3], 'w')
    tokens = generate_tokens(f1)
    trigram_alignments = generate_trigram_alignments(f2)
    output = {}
    alignments = ''
    for ( (index, length) , v) in trigram_alignments.iteritems():
        trigram = generate_trigram(index, length, tokens)
        for alignment in v:
            alignments += alignment + '\t'
        output[trigram] = alignments.strip()
    for trigram in sorted(output.iterkeys()):
        alignment = output[trigram]
        f3.write(trigram + '\t' + alignment + '\n')
    f1.close()
    f2.close()
    f3.close()

if __name__ == "__main__":
    sys.exit(main())
