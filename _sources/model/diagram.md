# R2X diagram

```{mermaid}
    flowchart TB
        input([Input Model]) --"Read model data"--> parser

        subgraph R2X["Artex Framework"]
            exporter[R2X exporter]
            parser[R2X parser]
            model[(DataModel)]
            updatedmodel[(Updated\nDataModel)]
            ext[Extensions]

            parser --"Create DataModel"--> model
            model .-> ext
            model --> exporter
            ext --> updatedmodel
            updatedmodel --> exporter
            ext --"Model configuration"--> exporter
        end

        exporter  --"Create output model"---> om(["Output DataModel"])
```
