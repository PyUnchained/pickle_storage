# Pickle-based Binary Data Storage

Based on the concept of storage containers used with the Kivy platform, this package allows users a convenient way to read and write arbitrary Python data using the pickle library. 

## Safety Warning

Pickling Python objects presents an inherent security risk: any valid code placed within the data can be run, including maliciously injected code. The package makes use of HMAC digests of the data to mitigate against accidental corruption, but when dealing with pickles it's always worth reiterating the seurity threat it poses in general

