schema = {'a' : (0,10), 'b' : (1,10000000)}                     
tbl = table(schema, nb=1000000)                                  
fichier = "/tmp/tbl.table"                                     
mem_sur_disque(tbl, fichier)                                
idx = index_fichier(fichier, 'a')                              
b0 = trouve_sur_disque("/tmp/tbl.table",idx[0])                    
print(sum(t['b'] for t in b0))
