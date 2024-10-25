class PartitionNameGenerator:
    @staticmethod
    def generate_partition_name(date) -> str:
        return f"{date.strftime('%b').lower()}{date.year}"
