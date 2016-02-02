index = 0
jobs = [1,2,3,4]
for job in jobs:
    if jobs[index] == 2 or jobs[index] == 4:
        del jobs[index]
    index += 1
    
print jobs