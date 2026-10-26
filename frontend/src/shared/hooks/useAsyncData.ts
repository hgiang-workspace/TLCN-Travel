import {useEffect,useState} from 'react'
export function useAsyncData<T>(load:()=>Promise<T>){const[data,setData]=useState<T>();const[error,setError]=useState(false);useEffect(()=>{let active=true;load().then(v=>{if(active)setData(v)}).catch(()=>{if(active)setError(true)});return()=>{active=false}},[load]);return{data,error,loading:!data&&!error}}
