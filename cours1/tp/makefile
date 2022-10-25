SOURCE = correction.py
TEST = projection transformation projection2 union selection selection_index appariement produit_cartesien_fichier jointure_theta jointure_naturelle jointure_naturelle_mem jointure_index jointure_double_index jointure_triee minimum_table moyenne_table ecart_type_table


${TEST}: %: test_%.py ${SOURCE}
	python3 $<

all: ${TEST}

.PHONY: ${TEST} all
