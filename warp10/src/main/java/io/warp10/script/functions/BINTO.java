//
//   Copyright 2016  Cityzen Data
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.script.functions;

import io.warp10.continuum.gts.UnsafeString;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

import org.bouncycastle.util.encoders.Hex;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;

/**
 * Decode a String in binary
 */
public class BINTO extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public BINTO(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object o = stack.pop();
    
    if (!(o instanceof String)) {
      throw new WarpScriptException(getName() + " operates on a String.");
    }
    
    String bin = o.toString();
    
    if (bin.length() % 8 != 0) {
      bin = "00000000".substring(bin.length() % 8) + bin;
    }
    
    char[] chars = UnsafeString.getChars(bin);
    
    StringBuilder sb = new StringBuilder();

    byte curbyte = 0;
    
    byte[] bytes = new byte[bin.length() / 8];
    int byteidx = 0;
    
    for (int i = 0; i < bin.length(); i += 4) {
      
      curbyte <<= 4;
      
      String nibble = new String(chars, i, 4);
      
      if ("0000".equals(nibble)) {
        curbyte |= 0;
      } else if ("0001".equals(nibble)) {
        curbyte |= 0x1;
      } else if ("0010".equals(nibble)) {
        curbyte |= 0x2;
      } else if ("0011".equals(nibble)) {
        curbyte |= 0x3;
      } else if ("0100".equals(nibble)) {
        curbyte |= 0x4;
      } else if ("0101".equals(nibble)) {
        curbyte |= 0x5;
      } else if ("0110".equals(nibble)) {
        curbyte |= 0x6;
      } else if ("0111".equals(nibble)) {
        curbyte |= 0x7;
      } else if ("1000".equals(nibble)) {
        curbyte |= 0x8;
      } else if ("1001".equals(nibble)) {
        curbyte |= 0x9;
      } else if ("1010".equals(nibble)) {
        curbyte |= 0xA;
      } else if ("1011".equals(nibble)) {
        curbyte |= 0xB;
      } else if ("1100".equals(nibble)) {
        curbyte |= 0xC;
      } else if ("1101".equals(nibble)) {
        curbyte |= 0xD;
      } else if ("1110".equals(nibble)) {
        curbyte |= 0xE;
      } else if ("1111".equals(nibble)) {
        curbyte |= 0xF;
      }
      
      if (i > 0 && 4 == i % 8) {
        bytes[byteidx++] = curbyte;
        curbyte = 0;
      }
    }

    stack.push(bytes);
    
    return stack;
  }
}
