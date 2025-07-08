# R2X Workflow Diagram

```{mermaid}
    flowchart TB
        input([Input Model]) --"Read Model Data"--> parser

        subgraph R2X["R2X Framework"]
            exporter[R2X exporter]
            parser[R2X Parser]
            model[(Data Model)]
            updatedmodel[(Updated Data Model)]
            ext[Plugins]

            parser --"Create Data Model"--> model
            model .-> ext
            model --> exporter
            ext --> updatedmodel
            updatedmodel --> exporter
            ext --"Model configuration"--> exporter
        end

        exporter  --"Create Output Model"---> om(["Output Data Model"])
```
