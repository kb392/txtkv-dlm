//#define RSL_EXTERN_IMPL
#include <io.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
//#include <locale>
//#include <string>
//#include <fstream>
//#include <iostream>
#include <Windows.h>
//#include "mssup.h"
#include "rsl/dlmintf.h"



char * rsGetStringParam(int iParam, char * defStr) {
    VALUE *vString;
    if (!GetParm (iParam,&vString) || vString->v_type != V_STRING) {
        if(defStr)
            return defStr;
        else
            RslError("Параметр №%i должен быть строкой",(iParam+1));
        }
    return vString->value.string;
    }

char * rsGetFilePathParam(int iParam) {
    VALUE *vFilePath;
    if (!GetParm (iParam,&vFilePath) || vFilePath->v_type != V_STRING)
        RslError("Параметр №%i должен быть строкой",(iParam+1));
    char * sPath=(char *)malloc(sizeof(char)*strlen(vFilePath->value.string)+sizeof(char));
    OemToCharBuff(vFilePath->value.string, sPath, strlen(vFilePath->value.string));
    sPath[strlen(vFilePath->value.string)]='\0';
    return sPath;
    }

// номер позиции указанного символа в буфере определённой длины, если символа нет, то длина буфера
int memchri(const char * buff, char c, int buff_len) {
    for (int i=0;i<buff_len;i++) {
        if (buff[i]==c)
            return i;
        }
    return buff_len;
    }
// количество вхождений символа в буфере
int memcnt(const char * buff, char c, int buff_len) {
    int cnt=0;
    for (int i=0;i<buff_len;i++) 
        if (buff[i]==c)
            cnt++;
    return cnt;
    }


// юникодные переводы строк не поддерживаются
int strcrlf(const char * buff, int buff_len, int * piStrSepLen) {
    *piStrSepLen=0;
    for (int i=0;i<buff_len;i++) {
        if (buff[i]=='\r' || buff[i]=='\n') {
            if (buff[i]=='\r' && i+1<buff_len && buff[i+1]=='\n') {
                * piStrSepLen=2;
                return i;
                }
            * piStrSepLen=1;
            return i;
            }
        }

    return -1;
    }

class  TTxtKV {
    
    int openFile(void) {
        if (fh>0)
            return 1;

        if (!*m_fileName.value.string)
            return 0;

        fh = _open(m_fileName.value.string, _O_RDONLY | _O_BINARY); // C4996
        if( fh == -1 )
            RslError("file %s error %i",m_fileName.value.string,errno);

        file_size = _lseeki64(fh, 0, SEEK_END);
        _lseeki64(fh, 0, SEEK_SET);

        setFileParams();

        return 1;

        }

    void setFileParams(void) {

        if (fh<0) 
            return;

        char * strMaxBuff=(char*)iNewMem(maxStringLength);

        if(_read(fh,strMaxBuff,maxStringLength)<=0)
            RslError("file %s error %i", m_fileName.value.string, errno);

        lenStr=strcrlf(strMaxBuff, maxStringLength, &lenStrSep);
        if (lenStr==0)
            RslError("Ошибка определения размера строки");

        key_length=memchri(strMaxBuff,separator,lenStr);
        firstKey=(char*)iNewMem(key_length+1);
        memcpy(firstKey,strMaxBuff,key_length);
        firstKey[key_length]='\0';

        currentKey=(char*)iNewMem(key_length+1);
        memcpy(currentKey,firstKey,key_length+1);

        // определение количества строк
        lines_count=file_size/(lenStr+lenStrSep);
        if (file_size % (lenStr+lenStrSep))
            lines_count++;

        if (lines_count==0)
            RslError("Ошибка. Пустой файл");

        count_fields=memcnt(strMaxBuff, separator,lenStr);

        __int64 pos = (lines_count-1)*(lenStr+lenStrSep);
        if (pos<0)
            RslError("Ошибка. Пустой файл");

        if (_lseeki64(fh, pos, SEEK_SET)!=pos)
            RslError("Ошибка позиционирования");

        if(_read(fh,strMaxBuff,lenStr)<=0)
            RslError("read file %s error %i", m_fileName.value.string, errno);

        int key_lengthTmp=memchri(strMaxBuff,separator,lenStr);

        if (key_lengthTmp!=key_length)
            RslError("Ошибка определния длины ключа");

        lastKey=(char*)iNewMem(key_length+1);
        memcpy(lastKey,strMaxBuff,key_length);
        lastKey[key_length]='\0';

        iDoneMem(strMaxBuff);

        strBuff   =(char*)iNewMem(lenStr+1);

        }

    bool GetKey(__int64 iLine) {
        if (fh<0) return FALSE;

        __int64 pos=(iLine-1)*(lenStr+lenStrSep);
        if (pos<0)
            return FALSE;

        if (_lseeki64(fh, pos, SEEK_SET)!=pos)
            RslError("Ошибка позиционирования");

        if(_read(fh,currentKey,key_length)<key_length)
            RslError("read key %i from file %s error %i", (int)iLine, m_fileName.value.string, errno);

        currentKey[key_length]='\0';

        return TRUE;
        }


    bool GetDataStr(__int64 iLine) {

        if (fh<0) return FALSE;

        int lenData=lenStr-key_length-1;

        if(resultStr==NULL)
            resultStr=(char *)iNewMem(lenData+1);

        __int64 pos=(iLine-1)*(lenStr+lenStrSep)+key_length+1;
        if (pos<0)
            return FALSE;

        if (_lseeki64(fh, pos, SEEK_SET)!=pos)
            RslError("Ошибка позиционирования");

        if(_read(fh, resultStr, lenData)<lenData)
            RslError("read str %i from file %s error %i", (int)iLine, m_fileName.value.string, errno);

        strBuff[lenStr]='\0';

        //currentKey=SubStr(currentString,1,key_length);
        return TRUE;
        }


    bool findKey(const char * key) {
        bool ret=TRUE;

        char *topKey=(char *)iNewMem(key_length+1);
        memcpy(topKey, firstKey, key_length+1);
        
        char *bottomKey=(char *)iNewMem(key_length+1);
        memcpy(bottomKey, lastKey, key_length+1);

        __int64 line;
        __int64 pevLine=-1;
        step=0;
        __int64 topLine=0;
        __int64 bottomLine=lines_count;

        line=lines_count/2;
        while(TRUE) {
            //Yprint("string #%I64d\n",line);
            if (!GetKey(line+1)) {
                ret=FALSE;
                break;
                }

            int r=memcmp (key, currentKey, key_length);

            if  (r==0) {
                GetDataStr(line+1);
                ret=TRUE;
                break;
                }
            else if(r<0) {
                memcpy(bottomKey, currentKey, key_length+1);
                bottomLine=line;
                }
            else if(r>0) {
                memcpy(topKey, currentKey, key_length+1);
                topLine=line;
                }
            else
                RslError("internal error");

            line=topLine+(bottomLine-topLine)/2;

            if(pevLine==line) {
                if (line==(lines_count-1) && 0==memcmp(key,lastKey,key_length)) {
                    line=lines_count;
                    continue;
                    }
                ret=FALSE;
                break;
                }

            pevLine=line;
            step++;

            } // while
        iDoneMem(topKey);
        iDoneMem(bottomKey);
        return ret;
        }


public:

    TTxtKV (TGenObject *pThis = NULL) {
        
      ValueMake (&m_fileName);
      ValueSet (&m_fileName,V_STRING,"");
      }

    ~TTxtKV () {
        if (fh>-1)
            _close(fh); // уходя закройте файл
        if (*m_fileName.value.string)
            free(m_fileName.value.string);
        ValueClear(&m_fileName);

        if (firstKey)   iDoneMem(firstKey);
        if (lastKey)    iDoneMem(lastKey);
        if (strBuff)    iDoneMem(strBuff);
        if (currentKey) iDoneMem(currentKey);
        if (resultStr)  iDoneMem(resultStr);
        
        }

    RSL_CLASS(TTxtKV)

    RSL_INIT_DECL() {         // void TTxtKV::Init (int *firstParmOffs)
        m_fileName.value.string=rsGetFilePathParam(*firstParmOffs);
        char * separator_param=rsGetStringParam(*firstParmOffs+1, "\t");
        separator=*separator_param;

        openFile();
        }

    RSL_METHOD_DECL(Open) {

        return 0;
        }

    RSL_GETPROP_DECL(CountFields) {
        ValueClear (retVal);
        ValueSet (retVal,V_INTEGER,&count_fields);
        return 0;
        }

    RSL_GETPROP_DECL(KeyLength) {
        ValueClear (retVal);
        ValueSet (retVal,V_INTEGER,&key_length);
        return 0;
        }

    RSL_GETPROP_DECL(StringLength) {
        ValueClear (retVal);
        ValueSet (retVal,V_INTEGER,&lenStr);
        return 0;
        }

    RSL_GETPROP_DECL(Separator) {
        char RsSeparator[2];
        RsSeparator[0]=separator;
        RsSeparator[1]='\0';

        ValueClear (retVal);
        ValueSet (retVal,V_STRING,RsSeparator);
        return 0;
        }

    RSL_GETPROP_DECL(CountLines) {

        //long double dbl=lines_count;
        ValueClear (retVal);
        //retVal->v_type=V_DOUBLEL;
        //retVal->value.doubvalL=(long double)lines_count;
        retVal->v_type=V_DOUBLE;
        retVal->value.doubval=(double)lines_count;
        //ValueSet (retVal,V_DOUBLEL,&dbl); //doubvalL
        return 0;
        }

    RSL_GETPROP_DECL(CountSteps) {

        ValueClear (retVal);
        ValueSet (retVal,V_INTEGER,&step);
        return 0;
        }

        


    RSL_METHOD_DECL(Find) {
        bool ret_bool=FALSE;
        const char * key =rsGetStringParam(1, NULL);

        if (findKey(key)) {
            if (count_fields)
                if (count_fields>1) {
                    TGenObject * a=RslTArrayCreate (count_fields,1);
                    int indexElement=0;
                    char *p=resultStr;
                    for(int i=0;resultStr[i];i++) {
                        if (resultStr[i]==separator) {
                            resultStr[i]='\0';
                            VALUE * pArrayElement=PushValue(NULL);
                            ValueSet (pArrayElement,V_STRING,(void *)p); 
                            RslTArrayPut(a, indexElement++, pArrayElement);
                            PopValue();
                            p=resultStr+i+1;
                            }
                        }
                            VALUE * pArrayElement=PushValue(NULL);
                            ValueSet (pArrayElement,V_STRING,(void *)p); 
                            RslTArrayPut(a, indexElement++, pArrayElement);
                            PopValue();

                    ValueSet (retVal,V_GENOBJ,a);
                    }
                else
                    ValueSet (retVal,V_STRING,resultStr);
            else {
                ret_bool=TRUE;
                ValueSet (retVal,V_BOOL,&ret_bool);
                }
            }
        else {
            ValueSet (retVal,V_BOOL,&ret_bool);
            }

        return 0;
        }
                  


private:
    VALUE m_fileName;
    int fh=-1; // file handle 
    __int64 file_size=0;
    int maxStringLength=256;
    int lenStr=-1;
    int lenStrSep=-1;
    char separator='\t';
    int key_length;
    char * firstKey  =NULL;
    char * lastKey   =NULL;
    int count_fields  =-1;
    char * strBuff   =NULL;
    char * currentKey=NULL;
    char * resultStr =NULL;
    int step=0;
    
    __int64 lines_count=0;
};

TRslParmsInfo prmOneStr[] = { {V_STRING,0} };

RSL_CLASS_BEGIN(TTxtKV)
    RSL_INIT
    RSL_PROP_EX    (fileName,m_fileName,-1,V_STRING, VAL_FLAG_RDONLY)
    RSL_METH_EX    (Find,-1,V_UNDEF,0,RSLNP(prmOneStr),prmOneStr)
    RSL_PROP_METH  (CountFields)
    RSL_PROP_METH  (Separator)
    RSL_PROP_METH  (KeyLength)
    RSL_PROP_METH  (CountLines)
    RSL_PROP_METH  (StringLength)
    RSL_PROP_METH  (CountSteps)
    
RSL_CLASS_END  


EXP32 void DLMAPI EXP AddModuleObjects (void) {
    //setlocale(LC_ALL,".866");
    RslAddUniClass (TTxtKV::TablePtr,true);
    }




