from utils import pic_load, pic_save


def filetype(file, type_):
    if type_ == 'simple':
        print('shit')
        file.set_saver(pic_save)
        file.set_loader(pic_load)
        file.set_func(lambda x: x)