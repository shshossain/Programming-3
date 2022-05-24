import argparse as ap
from pathlib import Path
import pickle
from Bio import Entrez

Entrez.email = "masfi.cs@gmail.com"
api_key='f1a7da656f5d4442111d3e7fdc19fe74b108'

def directory():
    directory = Path(__file__).parent.absolute()
    output = directory / 'output'
    try:
        output.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        print("Ouput Folder already exists")
    return output


def ncbi_references(pmid):
    results = Entrez.read(Entrez.elink(dbfrom="pubmed",
                                   db="pmc",
                                   LinkName="pubmed_pmc_refs",
                                   id=pmid,
                                   api_key='f1a7da656f5d4442111d3e7fdc19fe74b108'))
    references = [f'{link["Id"]}' for link in results[0]["LinkSetDb"][0]["Link"]]
    return references


def ncbi_authors(pmid):
    results = Entrez.read(Entrez.esummary(db="pubmed",id=pmid,
                                        api_key='f1a7da656f5d4442111d3e7fdc19fe74b108'))
    authors = []
    try:
        authors = [author for author  in results[0]["AuthorList"]]
        authors_tuples = tuple(authors)
    except RuntimeError:
        authors_tuples = (None)
    return authors_tuples

def pickle_module(pmid):
    
    output = directory()
    authors_tuples = ncbi_authors(pmid)
    with open(f'{output}/{pmid}.authors.pickle', 'wb') as file:
        pickle.dump(authors_tuples, file)

if __name__ == '__main__':
    argparser = ap.ArgumentParser(
                                description="Script that downloads (default) 10 articles referenced by the given PubMed ID concurrently.")
    argparser.add_argument("-n", action="store",dest="n", required=False, type=int,
                           help="Number of references to download concurrently.")
    argparser.add_argument("pubmed_id", action="store", type=str, nargs=1,
                             help="Pubmed ID of the article to harvest for references to download.")
    args = argparser.parse_args()
    print("Getting: ", args.pubmed_id)
    pmid =  str(args.pubmed_id)
    references = ncbi_references(pmid)
    for pmid in references:
        pickle_module(pmid)
