import pickle

# Pickle Save: file: str  <- obj: any
def pic_save(file:str, obj):
    with open(file, 'wb') as f:
        pickle.dump(obj, f)


def pic_load(file:str):
    with open(file, 'rb') as f:
        return pickle.load(f)