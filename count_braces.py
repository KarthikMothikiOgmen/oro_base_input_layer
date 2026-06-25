import sys
balance = 0
with open(sys.argv[1], 'r') as f:
    for i, line in enumerate(f):
        # simple count, ignore strings/comments for a rough estimate
        # wait, let's remove comments before counting
        line = line.split('//')[0]
        for char in line:
            if char == '{': balance += 1
            elif char == '}': 
                balance -= 1
                if balance == 0:
                    print(f"Balance reached 0 at line {i+1}")
