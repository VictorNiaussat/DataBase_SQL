
table = [{"a": 1, "b": 2, "c": 3},
                 {"a": 1, "b": 3, "c": 4},
                 {"a": 5, "b": 2, "c": 3},
                 {"a": 1, "b": 8, "c": 4},
                 {"a": 5, "b": 2, "c": 3}
                 ]
champs = ["a", "c"]

def projection(table, champs):
    """
    Renvoie la table (sous forme de flux) obtenue à partir des tuples contenus
    dans ~table~ en n'y conservant que les attributs (les clés) qui sont
    contenus dans ~champs~.

    Renvoie une exception si un attribut de ~champs~ n'est pas un attribut des
    tuples de ~table~.
    """
    for item in range(table):
        for item_clear in item.keys().__xor__(champs):
            del item[item_clear]
        yield item
        
print(projection(table, champs))