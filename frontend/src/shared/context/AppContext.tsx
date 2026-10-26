import {createContext,useContext,useMemo,useState,type ReactNode} from 'react'
import type {GeoContext,Place} from '../../types/domain'
interface Value{geo:GeoContext;setGeo:(v:GeoContext)=>void;comparison:Place[];toggleComparison:(p:Place)=>void}
const Context=createContext<Value|null>(null)
export function AppProvider({children}:{children:ReactNode}){const[geo,setGeo]=useState<GeoContext>({province:'Đà Nẵng',period:'09/2026'});const[comparison,setComparison]=useState<Place[]>([]);const value=useMemo(()=>({geo,setGeo,comparison,toggleComparison:(place:Place)=>setComparison(current=>current.some(item=>item.id===place.id)?current.filter(item=>item.id!==place.id):current.length<3?[...current,place]:current)}),[geo,comparison]);return <Context.Provider value={value}>{children}</Context.Provider>}
// This colocated hook is the typed public accessor for the provider above.
// eslint-disable-next-line react-refresh/only-export-components
export function useAppContext(){const value=useContext(Context);if(!value)throw new Error('AppProvider is required');return value}
