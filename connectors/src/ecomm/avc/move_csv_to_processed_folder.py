from utils.common import Task
from utils.aws_connection import AWSConnection


class MoveToProcessedFolderTask(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.source = self.get_source_from_table_name()
        self.aws_connect=AWSConnection()

    def get_source_from_table_name(self):
        if self.table_name.startswith('ryc_'):
            return "ryc"
        elif self.table_name.startswith('shp_apthk_'):
            return "shp_apthk"
        else:
            return "avc"

    def launch(self):
        if self.table_name == "flywheel_ads":
            self.aws_connect.move_previous_csv_to_processed_folder([], "flywheel", self.table_name)
        else:
            self.aws_connect.move_previous_csv_to_processed_folder([], self.source, self.table_name)


def entrypoint():
    task = MoveToProcessedFolderTask()
    task.launch()


if __name__ == "__main__":
    entrypoint()
