import os
from filetypes import filetype


class Folder(object):
    """Construct or take a folder with ops.

    Access Registered Files: Create, Delete, Show
        FolderA.file1
        FolderA.file2
        ...


    File Pipeline: Create, Delete, Show
        FolderA['Pipeline1'] = (file1, file2, ..., fileN)
        FolderA['Pipeline2'] = (Pipeline1, fileK, ..., fileM)

    """
    def __init__(self, name, parent_path=None):
        self.name = name
        if parent_path is None:
            parent_path = os.getcwd()
        else:
            if not os.path.exists(parent_path):
                raise IOError("Parent path doesn't exist for {}.".format(self.name))
        self.path = os.path.realpath(os.path.join(parent_path, self.name))

        self.parent_path = parent_path
        self.files = []
        self.pipelines = {}

        self._make()  # make this directory

    def _make(self):
        if not os.path.exists(self.path):
            os.makedirs(self.path)
        else:
            print("Already exists: {}.".format(self.path))

    @staticmethod
    def _list_files(startpath, limit=None):
        """print all the files in a directory with limit to non-directorial files."""
        for root, dirs, files in os.walk(startpath):
            level = root.replace(startpath, '').count(os.sep)
            indent = ' ' * 4 * level
            print('{}{}/'.format(indent, os.path.basename(root)))
            subindent = ' ' * 4 * (level + 1)
            count = 1
            for f in files:
                if limit is not None:
                    if f[-1] != r'/':
                        if count <= limit:
                            count += 1
                            print('{}{}'.format(subindent, f))
                        elif count == limit + 1:
                            count += 1
                            print('{}{}'.format(subindent, '......'))
                    else:
                        print('{}{}'.format(subindent, f))
                else:
                    print('{}{}'.format(subindent, f))

    # show all contents
    def show_content(self, limit=None):
        self._list_files(self.path, limit)

    # show registered files
    def show_files(self):
        print("Registered Files:")
        for file in self.files:
            print('\t'+file.name)

    def show_pipelines(self):
        print("Pipelines:")
        for pipeline in self.pipelines:
            print('\t'+pipeline)

    # implicit file registration and creation and access
    # files must have unique names
    def __getattr__(self, attr):
        for file in self.files:
            if file.name == attr:
                return file
        # else create one
        new_file = File(attr, parent_path=self.path)
        self.files.append(new_file)
        return new_file

    # delete a file
    def delete(self, name):
        for file in self.files:
            if file.name == name:
                self.files.remove(file)  # require name to be unique!
                del file

    # implicit pipeline construction and access
    def __getitem__(self, pipe):
        if pipe in self.pipelines:
            return self.pipelines[pipe]
        self.pipelines[pipe] = Pipeline(pipe)
        return self.pipelines[pipe]

    # delete a pipeline
    def destruct(self, pipe):
        for pipeline in self.pipelines:
            if pipeline.name == pipe:
                del self.pipelines[pipe]  # require name to be unique!


class File(object):
    """A facade for an actual file.

    File.fit(*input): generate this file using some inputs.

    File State Management:
        Protected: version protection
        Frozen: not mutable

    File Access Management:
        Load
        Save
        Inspect: show important statistics about the file
        Sample: generate a sample file from this file
    """
    def __init__(self, name, parent_path=None):
        self.frozen = False
        self.protected = False
        self.saver = None
        self.loader = None
        self.inspector = None
        self.sampler = None
        self.name = name
        if parent_path is None:
            parent_path = os.getcwd()
        else:
            if not os.path.exists(parent_path):
                raise IOError("Parent path doesn't exist for {}.".format(self.name))
        self.path = os.path.realpath(os.path.join(parent_path, self.name))

        self.parent_path = parent_path
        self.func = None

    def construct(self, func):
        self.func = func

    def fit(self, *inputs):
        obj = self.func(*inputs)
        self.save(obj)

    # write to path
    # saver(path, obj)
    def set_saver(self, saver):
        self.saver = saver

    # read from path
    # loader(path)
    def set_loader(self, loader):
        self.loader = loader

    # inspector(obj)
    def set_inspector(self, inspector):
        self.inspector = inspector

    # sampler(obj)
    def set_sampler(self, sampler):
        self.sampler = sampler

    # obj = func(*inputs)
    def set_func(self, func):
        self.func = func

    def save(self, obj):
        """Use writer function to create files.

        Only require name, not path.
        """

        if self.saver is None:
            raise TypeError("For {}, the saver has not been set.".format(self.name))
        try:
            self.saver(self.path, obj)
        except:
            print("Fail to add the file using writer.")
            raise

    def load(self):
        """Read file information.

        Only require name, not path.
        """
        if self.loader is None:
            raise TypeError("For {}, the loader has not been set.".format(self.name))
        try:
            return self.loader(self.path)
        except:
            print("Fail to extract the file using writer. Maybe it hasn't been fitted yet.")
            raise

    def inspect(self):
        if self.inspector is None:
            raise TypeError("For {}, the inspector has not been set.".format(self.name))
        self.inspector(self.load())

    def sample(self) -> object:
        if self.sampler is None:
            raise TypeError("For {}, the sampler has not been set.".format(self.name))
        return self.sampler(self.load())

    def astype(self, type_):
        filetype(self, type_)
        return self


class Pipeline(object):
    """Grouping of files sequentially.

    Pipe.fit(*input): fit all the files in the pipeline.

    """
    def __init__(self, name):
        self.name = name
        self.pipe = None

    def construct(self, *files: File):
        self.pipe = tuple(files)

    def fit(self,*inputs):
        # check pipeline validity
        if self.pipe is None:
            raise ValueError('This pipeline is empty.')
        # check file has function
        for file in self.pipe:
            if file.func is None:
                raise ValueError('{} doesn\'t have a function. Use set_func.'.format(file.name))

        obj = inputs
        for idx, file in enumerate(self.pipe):
            # if this and the next file is freezed, don't load this file
            if idx + 1 <= len(file) - 1 \
                    and self.pipe[idx+1].frozen \
                    and self.pipe[idx].frozen:
                continue
            elif self.pipe[idx].frozen:
                obj = file.load()
            else:
                obj = file.fit(*inputs)
        return obj

    def __str__(self):
        return '->'.join([file.name for file in self.pipe])