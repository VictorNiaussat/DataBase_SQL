import random
from disque import *

def flux_carres(n):
    """Flux des carrés de 0 à n-1"""
    for i in range(n):
        yield i*i

def somme_carres(n):
    """Calcule la somme des carrés de 0 à n-1."""
    res = 0
    for i in flux_carres(n):
        res += i
    return res

def somme_carres_bis(n):
    return sum([i*i for i in range(n)])

def somme_carres_ter(n):
    res = 0
    for k in (i*i for i in range(n)):
        res+=k
    return k

def somme_carres_quad(n):
    return sum(i*i for i in range(n))

def table(descr, nb=10000):
    """Cette fonction génère une séquence de tuples décrits par le dictionnaire
    descr. Le dictionnaire associe à une clé une paire (k,l). La fonction
    génère nb dictionnaires de la manière suivante :
    - chaque clé de descr est une clé de ces dictionnaires
    - à chacune de ces clés x, ces dictionnaires associent un nombre tiré au hasard
      entre k et l lorsque la paire (k,l) est associée à x dans descr.
    NB : cette fonction requiert d'importer le module random.
    """
    for _ in range(nb):
        tuple_res = {}
        for a, (k,l) in descr.items():
            tuple_res[a] = random.randint(min(k, l), max(k, l))
        yield tuple_res

def exemple_table():
    """Exemple d'utilisation de la fonction table. Génère une table de 10 éléments
comportant les attributs 'a' et 'b' et les affiche en flux."""
    schema = {'a': (0,10), 'b': (100,100000)}
    for tuple_tbl in table(schema,nb=10):
        print(tuple_tbl)

def projection(table, champs):
    """
    Renvoie la table (sous forme de flux) obtenue à partir des tuples contenus
    dans ~table~ en n'y conservant que les attributs (les clés) qui sont
    contenus dans ~champs~.

    Renvoie une exception si un attribut de ~champs~ n'est pas un attribut des
    tuples de ~table~.
    """
    yield {}

def exemple_projection():
    """Exemple d'utilisation de la projection."""
    schema = {'a': (1, 10), 'b': (40, 100), 'c': (20,30)}
    for tuple_res in projection(table(schema ,nb=100),
                                ['a', 'c']):
        print(tuple_res)

def transformation(table, f):
    """Renvoie un flux obtenu en appliquant ~f~ à chacun des tuples composant
~table~."""
    yield {}

def exemple_transformation():
    schema = {'a': (1, 10), 'b': (40, 100), 'c': (20,30)}
    f = lambda tp: {'a': tp['a'], 'm': (tp['b']+tp['c'])//2}
    for tuple_res in transformation(table(schema,nb=100), f):
        print(tuple_res)

def projection2(table, champs):

    """
    Renvoie la table (sous forme de flux) obtenue à partir des tuples contenus
    dans ~table~ en n'y conservant que les attributs (les clés) qui sont
    contenus dans ~champs~.

    Renvoie une erreur si un attribut de ~champs~ n'est pas un attribut des
    tuples de ~table~.
    """
    yield {}

def union(t1, t2):
    """Construit un flux qui énumère les éléments de ~t1~ puis ceux de ~t2~."""
    yield {}

def exemple_union():
    """Exemple d'utilisation de la fonction union."""
    schema1 = {'a':(30, 100), 'b': (10, 50)}
    schema2 = {'a': (40, 50), 'n': (100, 200), 'm': (0,10)}
    f = lambda tp: {'a': tp['a']//2, 'b': (tp['m']*tp['m'])//4}
    for tp in union(table(schema1, nb = 10),
                    transformation(table(schema2,nb=10),f)):
        print(tp)

def selection(table, pred):
    """Construit le flux des éléments de ~table~ qui satisfont le prédicat
       ~pred~ (fonction des tuples dans les booléens)."""
    yield {}

def exemple_selection():
    for un_tuple in selection(table({'a': (30, 100), 'b': (10, 50)}, nb=10),
               lambda tp: tp['a'] > 50 and tp['b'] < 45):
        print(un_tuple)

def selection_index(fichier, idx, valeurs) :
    """
    On suppose que ~fichier~ contient des tuples dont l'une des colonnes est
    indexée par ~idx~. La fonction renvoie le flux des tuples qui associe a la
    colonne indexée une valeur dans la séquence ~valeurs~.

    Attention : si un élément de ~valeurs~ n'est pas référencé dans ~idx~, on
    souhaite qu'il n'y ait pas d'erreur.
    """
    yield {}

def appariement(t1, t2):
    """Renvoie un tuple ayant pour clé les clés de ~t1~ et de ~t2~.

    Lorsqu'une clé n'apparaît que dans un tuple la valeur que lui associe ce
    tuple est celle associée à la clé dans le résultat.

    À une clé qui apparaît dans les deux tuples, le résultat associe la valeur
    que lui associe ~t2~.
    """
    return {}

def produit_cartesien(table1, table2):
    """Construit le flux de tuples obtenus en appariant tous les tuples de
    ~table1~ et de ~table2~.

    Ce flux correspond au produit cartésien des deux tables produit par l'algorithme double boucle :
    - ~table1~ est la table utilisée dans le boucle extérieure,
    - ~table2~ est la table utilisée dans la boucle intérieure.
    """
    yield {}

def produit_cartesien_fichier(fichier1, fichier2):
    """Construit le flux de tuples obtenus en appariant tous les tuples contenus
    dans les fichiers ~fichier1~ et ~fichier2~.

    Ce flux correspond au produit cartésien des deux tables contenues dans les
    fichiers produit par l'algorithme double boucle :
    - la table contenue dans ~fichier1~ est utilisée dans la boucle extérieure,
    - la table contenue dans ~fichier2~ est la table utilisée dans la boucle intérieure.

    """
    yield {}

def jointure_theta(fichier1, fichier2, pred):
    """Renvoie le flux des appariements de tuples contenus dans les tables des
    fichiers ~fichier1~ et ~fichier2~" qui satisfont la propriété du prédicat
    ~pred~ (fonction des tuples dans les booléens)."""
    yield {}

def jointure_naturelle(fichier1, fichier2):
    """Renvoie le flux des tuples de la jointure naturelle des tables contenues
    dans ~fichier1~ et ~fichier2~.

    Il s'agit des appariements des tuples provenant des tables contenues dans
    ~fichier1~et ~fichier2~ qui associent les mêmes valeurs à leurs attributs
    communs.
    """
    yield {}

def jointure_naturelle_mem(fichier1, fichier2):
    """Renvoie le flux des tuples de la jointure naturelle des tables contenues
    dans ~fichier1~ et ~fichier2~.

    L'un des deux fichiers est chargé en mémoire afin qu'il ne soit lu qu'une
    seule fois.

    Il s'agit des appariements des tuples provenant des tables contenues dans
    ~fichier1~et ~fichier2~ qui associent les mêmes valeurs à leurs attributs
    communs.

    """
    yield {}

def jointure_index(table1, col1, fichier2, index):
    """Renvoie le flux des tuples de la jointure de la ~table1~ et de la table
    contenue dans ~fichier2~ sous la condition que les valeurs de l'attribut
    ~col1~ de ~table1~ soient identiques aux valeurs de l'attribut ~col2~ la
    table de ~fichier2~.

    ~index~ est un index de l'attribut ~col2~ dans ~fichier2~.
    """
    yield {}

def jointure_double_index(fichier1, index1, fichier2, index2):
    """Renvoie le flux de tuples obtenue par la jointure des tables contenues dans
     ~fichier1~ et ~fichier2~ avec la condition que les valeurs indexées par
     ~index1~ pour ~table1~ soient égaux aux valeurs indexées par ~index2~ pour
     ~table2~."""
    yield {}

def jointure_triee(table1, col1, table2, col2):
    """Implémente la jointure de ~table1~ et ~table2~ sous la condition que les
    valeurs de l'attribut ~col1~ de ~table1~ soient égales aux valeurs de
    l'attribut ~col2~ de ~table2~.

    On suppose que ~table1~ est triée suivant les valeurs croissantes de ~col1
    et que ~table2~ est triée suivant les valeurs croissantes de ~col2~.
    """
    yield {}

def minimum_table(table, col):
    """Renvoie la plus petite des valeurs associées à l'attribut ~col~ dans ~table~.
    Si ~table~ est vide, renvoie None.
    """
    return None

def moyenne_table(table, col):
    """Renvoie la moyenne des valeurs que les tuples de ~table~ associent à
    l'attribut ~col~.
    Si ~table~ est vide, renvoie None.
    """
    return None

def ecart_type_table(table, col):
    """Renvoie l'écart type des valeurs que les tuples de ~table~ associent à
    l'attribut ~col~.
    Si ~table~ est vide, renvoie None.
    """
    return None
