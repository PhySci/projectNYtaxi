#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 25 11:33:39 2017

@author: frodos
"""


audio=file('/dev/audio', 'wb')

count=0

while count<250:
    beep=chr(63)+chr(63)+chr(63)+chr(63)
    audio.write(beep)
    beep=chr(0)+chr(0)+chr(0)+chr(0)
    audio.write(beep)
    count=count+1
    audio.close()