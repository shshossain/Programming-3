
import multiprocessing as mp
from Bio import Entrez
import argparse as ap
import time
from pathlib import Path


Entrez.api_key = 'f1a7da656f5d4442111d3e7fdc19fe74b108'
Entrez.email = 'masfi.cs@gmail.com'


def ncbi_references(pmid):
  results = Entrez.read(Entrez.elink(dbfrom="pubmed",
                                   db="pmc",
                                   LinkName="pubmed_pmc_refs",
                                   id=pmid,
                                   api_key='f1a7da656f5d4442111d3e7fdc19fe74b108'))
  references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
  
  return references


def ncbi_articles(pmid): 
    handle = Entrez.efetch(db="pmc", id=pmid, rettype="XML", retmode="text",
                            api_key='f1a7da656f5d4442111d3e7fdc19fe74b108')
    path = Path(__file__).parent.absolute()
    output_path = path/'output'
    if not(output_path.exists()):
      output_path.mkdir(parents=True,exist_ok=False)
    with open(f'{output_path}/{pmid}.xml', 'wb') as file:
        file.write(handle.read())

if __name__ == '__main__':
  
    argparser = ap.ArgumentParser(description="Script that downloads (default) 10 articles referenced by the given PubMed ID concurrently.")
    argparser.add_argument("-n", action="store",
                           dest="n", required=False, type=int,
                           help="Number of references to download concurrently.")
    argparser.add_argument("pubmed_id", action="store", type=str, nargs=1, help="Pubmed ID of the article to harvest for references to download.")
    args = argparser.parse_args()
    print("Getting: ", args.pubmed_id)
    
    pmid =  str(args.pubmed_id)
    references = ncbi_references(pmid)
    
    with mp.Pool(4) as pool:
        results = pool.map(ncbi_articles, references[0:10])
    