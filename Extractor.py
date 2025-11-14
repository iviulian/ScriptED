import requests
import camelot
import pandas as pd
import os

   # Setarile pentru sursa si output
link_pdf = 'https://bvb.ro/Bilanturi/SNP/SNP_S_2023.pdf'
nume_fisier = 'SNP_S_2023.pdf'
folder_rezultate = 'Date_BVB_SNP_Extrase'

# Pun user Agent ca sa nu creda serverul ca sunt bot si sa iau block
headers_browser = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}


def download_logic(url, cale_fisier):
    print("Se Descarcă Fișierul "+cale_fisier)   # Functie simpla sa trag pdful de pe net

    try:
        raspuns=requests.get(url,headers=headers_browser,stream=True)
        
        # Verific daca requestul a fost ok 200
        raspuns.raise_for_status()
        
        with open(cale_fisier, 'wb') as f:
            for bucata in raspuns.iter_content(chunk_size=8192):
                f.write(bucata)
        
        marime_kb = os.path.getsize(cale_fisier) / 1024   # calculez mărimea in kb sa verific dacă e valid
        
        
        if marime_kb<100:
            # Dacă e sub 100kb probabil estr fisierul cu "Please wait..."
            print("Atentie: fisierul e prea mic (" + str(marime_kb) + " KB). poate fi o eroare.")
            return False
            

        print("Descarcare finalizata cu succes. marime: " + str(marime_kb) + " KB")
        return True
    
    except Exception as e:
        print("Eroare la descarcarea fisierului: " + str(e))
        return False




def procesare_tabele_excel(path_pdf, director_output):
    
    # Aici scot tabelele si le pun in excel pe sheet-uri separate
    print("\nIncepe extractia si consolidarea tabelelor din: " + path_pdf)
    
    os.makedirs(director_output, exist_ok=True)
    
    try:
        # Incerc prima data cu flavor stream ca e mai bun pt tabele fara linii
        tabele_gasite = camelot.read_pdf(path_pdf, pages='all', flavor='stream')
        
        
        
        
        # Verific daca am gasit ceva cu stream
        if tabele_gasite.n == 0:
             # Daca nu a mers stream incerc lattice pt tabele cu linii
             tabele_gasite = camelot.read_pdf(path_pdf, pages='all', flavor='lattice')
             if tabele_gasite.n == 0:
                  print("Nu au fost gasite tabele utilizabile.")
                  return
        
        
        print("Extractie reusita.Au fost gasite " + str(tabele_gasite.n))
        
        # Construiesc calea pt fisierul final
        nume_clean = os.path.basename(path_pdf).replace(".pdf", "")
        fisier_final = os.path.join(director_output, 'CONSOLIDAT_' + nume_clean + '.xlsx')
        
        
        # Folosesc excelwriter ca sa pot scrie in multiple sheets
        with pd.ExcelWriter(fisier_final) as writer:
            
            
            # Iterez prin toate tabelele gasite
            for i, tabel_curent in enumerate(tabele_gasite):
                df = tabel_curent.df
                

                # Fac putin clean la date scot randurile goale
                df.dropna(how='all', inplace=True)
                
                # Scot spatiile albe daca e string
                df =df.apply(lambda x: x.str.strip() if x.dtype=="object" else x)
                
                
                nume_sheet="Tabel "+str(i+1)+" (Pag. "+str(tabel_curent.page)+")"     # Fac numele la sheet din index si pagina
                             
                df.to_excel(writer, sheet_name=nume_sheet, index=False, header=False)
                print("Salvare in foaia:   " + nume_sheet)

        print("\nConsolidare finalizata! toate tabelele sunt in: " + fisier_final) 

    except Exception as e: print("\nEroare in timpul consolidarii: " + str(e))





if __name__ == "__main__":
   #  Mai intai descarc si daca e bine trec mai departe la extractie
    if download_logic(link_pdf, nume_fisier):
        # Apelez functia de procesare
        procesare_tabele_excel(nume_fisier, folder_rezultate)
