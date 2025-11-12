# ray_velda

Support Ray cluster auto-scaling running on dev instances powered by Velda.

Usage
-----

1. Setup a ray cluster config. See [ray_config_example.yaml](/ray_config_example.yaml) for example.
2. To start a cluster:

```sh
ray up ray_config.ayml
```

3. After your cluster is up, you can

* Access Ray dashboard: `http://8265-ray-[cluster-name]-[instance-id].i.[velda-domain]`
* Submit job: `ray job submit --address http://ray-[cluster-name]:8265 -- python my_script.py`