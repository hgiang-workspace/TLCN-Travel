import {places} from '../data/mockData'
import type {DomainId,Place} from '../types/domain'
export interface TravelDataService{searchPlaces(query:string,domain:DomainId):Promise<Place[]>;getPlace(id:string):Promise<Place|undefined>}
const wait=(ms=280)=>new Promise(r=>window.setTimeout(r,ms))
class MockTravelDataService implements TravelDataService{async searchPlaces(query:string,domain:DomainId){await wait(360);const q=query.trim().toLocaleLowerCase('vi');return places.filter(p=>(domain==='all'||p.domain===domain)&&(!q||(p.name+' '+p.match+' '+p.location).toLocaleLowerCase('vi').includes(q)))}async getPlace(id:string){await wait();return places.find(p=>p.id===id)}}
export const travelDataService:TravelDataService=new MockTravelDataService()
