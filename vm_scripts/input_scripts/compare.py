import sys

if __name__ == '__main__':
    
    f1 = str(sys.argv[1])
    f2 = str(sys.argv[2])
    
    file1 = open(f1, 'r')
    lines1 = file1.readlines()

    file2 = open(f2, 'r')
    lines2 = file2.readlines()

    j=0
    for k in range(len(lines1)):
          if lines1[k]!= lines2[k]:
                print(lines1[k],lines2[k],'\n')
                j=j+1

    print(j)