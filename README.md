# map-reduce-test
Map Reduce home test

## Compile

    hadoop com.sun.tools.javac.Main FirstStep.java SecondStep.java ThirdStep.java && jar cf FirstStep.jar FirstStep*.class && jar cf SecondStep.jar SecondStep*.class && jar cf ThirdStep.jar ThirdStep*.class

## Run

    hadoop jar FirstStep.jar FirstStep input2.tsv FirstStep && hadoop jar SecondStep.jar SecondStep FirstStep/part-r-00000 SecondStep && hadoop jar ThirdStep.jar ThirdStep SecondStep/part-r-00000 ThirdStep

Results will be placed in `ThirdStep/part-r-00000`

    cat ThirdStep/part-r-00000
