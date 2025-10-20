import typing
defdict = dict[str, typing.Any]
optdefdict = defdict | None

dictlist = list[defdict]
optdictlist = dictlist | None