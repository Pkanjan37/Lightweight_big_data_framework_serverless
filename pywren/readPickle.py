import pickle

objects = []
with (open("/media/p/Elements/ChromeDownload/aggdata.pickle", "rb")) as openfile:
    while True:
        try:
            objects.append(pickle.load(openfile))
        except EOFError:
            break
print(objects)
