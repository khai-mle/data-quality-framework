import os

class FileDirectory:
    def __init__(self, folder, file = None):
        self.folder = folder
        self.file = file

        self.cwd = os.path.abspath(os.getcwd())
        self.folder_path = os.path.join(self.cwd,self.folder)

    def curr_dir(self):
        return self.cwd
    
    def list_dir(self):
        return os.listdir(self.folder_path)

    def file_path(self):
        return os.path.join(self.folder_path,self.file)