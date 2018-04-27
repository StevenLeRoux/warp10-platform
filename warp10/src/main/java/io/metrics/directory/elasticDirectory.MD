# Set-up

This directory plugin need to be deployed as a directory. You can remove HBase update with the Warp 10 param in conf:

Add also a plugin directory param:
```
directory.plugin.class = io.metrics.directory.DirectoryES
```

When pushing the series, this plugin will delete all the labels/attribute starting with ".". If the series contains a labels/attributes equals to "_type", "_index" or "_id", it will be removed.

The class of the series is pushed in the document using an internal param as key (directory.es.classname) by default it corresponds to "explorer_classname".

# Internal parameter

This plugin add a few parameter with:

```
# The main param to create an index, by default .app
directory.es.app
```

```
# The key under the classname will be pushed in Elastic, by default .explorer_classname
directory.es.classname
```

For the other param, the default value has been set to work correctly on a VM with 60G of ram and 30 for elastic. 4M of dp have been correctly pushed in less than 5 minutes.