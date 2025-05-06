import wool


@wool.cli.group()
def lockset():
    pass


@lockset.command()
def up(): ...


@lockset.command()
def down(): ...
