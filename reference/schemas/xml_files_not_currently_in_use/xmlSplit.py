import sys

def main():
    print('Splitter created by Dan Peterson')
    print('Yea, I know it''s ugly but it works! :-)')

    f = open("XMLSchema_255varchar_and_correctKeys.txt","r")
    f1 = f.readlines()
    for x in f1:
        #print('Reading line: ' + x)
        if ".xml" in x:
            print('Creating or updating file: ' + x)
            subf= open(x[:-1],"w+")
        elif "<?xml" or "<column" or "<schema" in x:
            subf.write(x)
        elif x == "</schema>":
            subf.write(x)
            subf.close()
        else:
            print('line is empty')
        
    f.close

if __name__== "__main__":
      main() 


