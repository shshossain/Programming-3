#!/bin/bash
#!/bin/bash
#SBATCH --mail-type=ALL
#SBATCH --mail-user=sh.s.hossain@st.hanze.nl
#SBATCH --time 48:00:00
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --job-name=hossain_assignment4
#SBATCH --partition=assemblix



export FILE1=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R1_001_BC24EVACXX.filt.fastq
export FILE2=/data/dataprocessing/MinIONData/MG5267/MG5267_TGACCA_L008_R2_001_BC24EVACXX.filt.fastq
export OUTPUT=/students/2021-2022/master/Hossain/output

mkdir -p /students/2021-2022/master/Hossain/output
mkdir -p output

# Run velveth and store output in student folder 
seq 25 2 31 | parallel -j16 'velveth $OUTPUT/{} {} -longPaired -fastq $FILE1 $FILE2 && velvetg $OUTPUT/{} && cat $OUTPUT/{}/contigs.fa | (python3 assignment4.py && echo -e Kmer_size:{}; ) >> output/output.csv'

# Run python file for cleaning
python3 best.py

# removing files
rm -rf $OUTPUT