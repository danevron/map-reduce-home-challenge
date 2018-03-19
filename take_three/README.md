## How it works

The solution consists of 4 Map Reduce jobs.

In a high level, it joins (self join) the sites according to tags, creates all pairs of sites per tag, sums the pairs and sorts them with secondary sort.

First job - Groups all sites per tag.
Second job - Creates pairs of sites and partly sums them.
Third job - Finishes to sum the pairs and prepare them for sorting.
Fourth job - Performs the secondary sort and return the requested format.

## Compile

    hadoop com.sun.tools.javac.Main FirstStep.java SecondStep.java ThirdStep.java FourthStep.java && jar cf FirstStep.jar FirstStep*.class && jar cf SecondStep.jar SecondStep*.class && jar cf ThirdStep.jar ThirdStep*.class && jar cf FourthStep.jar FourthStep*.class

## Run

    hadoop jar FirstStep.jar FirstStep input2.tsv FirstStep && hadoop jar SecondStep.jar SecondStep FirstStep/part-r-00000 SecondStep && hadoop jar ThirdStep.jar ThirdStep SecondStep/part-r-00000 ThirdStep && hadoop jar FourthStep.jar FourthStep ThirdStep/part-r-00000 FourthStep

Results will be placed in `FourthStep/part-r-00000`

    cat FourthStep/part-r-00000
