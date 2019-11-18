import os

train_dir = "./images/train"
test_dir = "./images/test"

ld = os.listdir(train_dir)

for f in ld:
    if ".xml" not in f:
        title = f.split(".")[0]
        if title + ".xml" not in ld:
            os.remove(train_dir + "/" + f)