# System Validation

In order to validate the system’s features, functionality and performance, a suitable series of test observations were carried out. Since MeerKAT does not currently offer X-band receivers, the Voyager spacecraft cannot be used as technosignature test signal sources as they have been in prior SETI surveys. Instead, we observed the James Webb Space Telescope’s S-band telemetry downlink with MeerKAT’s S-band receivers. JWST is a goodnarrowband technosignature test source as it moves relatively slowly in right ascension and declination, remaining well within the primary field of view over a 290 second recording/processing cycle.

To conduct an end-to-end test of the entire pipeline, we observed a fixed pointing in right ascension and declination in the path of JWST, recording its short transit across a small portion of the primary field of view. We observed using MeerKAT’s S0 band in 4k mode.

<img src="figures/jwst.gif" alt="jwst-gif" width=80%/>


The plot above illustrates JWST’s transit during the 290 second recording by means of a set of tiled synthesized beams placed at fixed sky coordinates. These beams are produced directly by the BLUSE pipeline. The next figure illustrates the change in power over time of several synthesized beams placed in the path of JWST's transit:

<img src="figures/beam-transit.pdf" alt="beam-transit" width=80%/>

We can also compare the coherent and incoherent beams for this observation to calculate an efficiency ratio as follows:

η = Pcoherent/Pincoherent/Nants

where Pcoherent and Pincoherent are normalised. This yields a ratio Pcoherent/Pincoherent of approximately 52.63, yielding an efficiency of 84.9 % given that there were 62 antennas in the subarray at the time of the observation.
