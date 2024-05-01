debiet_3di =>
tabel 'Debieten_3Di_HR', gelinkt op geometrie met hydroobjecten, in de juiste richting
Als hydroobject geen debiet heeft (NULL). ==> tabel 'Hydroobject', kolommen
debiet_opgelegd_m3s * debiet_afvoer_prof

debiet_inlaat => tabel 'Hydroobject', kolom debiet aanvoer

debiet_aangepast => herverdeling naar primair en om te zorgen dat alles
afvoert

debiet =>
debiet_opgelegd (handmatig gevulde kolom om alles te overrulen)
max(debiet_aangepast, debiet_3di)



